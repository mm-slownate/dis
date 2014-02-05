import os
import sys
import BaseHTTPServer
import SocketServer
import urllib
import threading

from urlparse import urlparse
from email import utils

import dis


class write_lease:
	def __init__(self, item, size):	# with lock
		self.item = item
		self.size = size

		self.fd = None
		self.bytes = 0
		self.expires = ''
		self.free_list = []

		assert not item.is_busy()
		item.rootnode.leases.append(item)

	def reserve(self):
		fsstat = os.statvfs(self.item.rootnode.path)
		return (float(fsstat.f_bavail) / fsstat.f_blocks) * 100

	def rsv_str(self):
		return str(self.reserve())[:4]

	def renew(self):	# with lock
		assert self.item.is_busy()
		assert self.bytes == 0

		if not self.item.is_empty():
			dis.pop(self.item)
		dis.insert(self.item)
		self.bytes = 2**18
		if not self.size is None:
			if self.size < self.bytes:
				self.bytes = self.size
			self.size -= self.bytes
		self.fd.flush()
		if self.reserve() > disreserve:
			return

		r = self.item.rootnode
		freed = 0
		while freed < self.bytes:
			item = r.get_prev()
			if item in r.leases:
				break
			assert not item.is_empty()
			dis.pop(item)
			del r.items[item.itemname]
			self.free_list.append(item)
			freed += os.path.getsize(item.path)

	def log_fields(self):
		yield "%d byte lease" % self.bytes
		yield "%s%% reserve" % self.rsv_str()
		if not self.size is None:
			yield "%dKB after this" % (self.size/2**10)
		if self.free_list:
			yield "freeing.."
		for item in self.free_list:
			yield item.itemname

	def reclaim_files(self):
		for item in self.free_list:
			os.remove(item.path)
		self.free_list = []

	def write(self, chunk):
		self.fd.write(chunk)
		self.bytes -= len(chunk)

	def close(self):	# with lock
		assert self.item.is_busy()
		self.fd.close()
		self.fd = None
		self.item.rootnode.leases.remove(self.item)


def mkdir_p_recursive(rootdir, itemdir):
	path = '/'.join([rootdir, itemdir])
	if not os.path.isdir(path):
		parent = os.path.dirname(itemdir)
		if parent:
			mkdir_p_recursive(rootdir, parent)
		os.mkdir(path)


def rmdir_p_iterative(rootdir, name):
	name = os.path.dirname(name)
	while name:
		try:
			os.rmdir('/'.join([rootdir, name]))
		except:
			break
		name = os.path.dirname(name)


def sanitize(path):
	cleanpath = []
	for c in path.split('/'):
		if not c or c == '.':
			continue
		if c == "..":
			cleanpath and cleanpath.pop()
		else:
			cleanpath.append(c)
	return '/'.join(cleanpath)


class dis_handler(BaseHTTPServer.BaseHTTPRequestHandler):
	def urlpath(self):
		try:
			url = urlparse(self.path)[2]
			path = urllib.unquote(url)
			cleanpath = sanitize(path)
		except:
			cleanpath = None
		return cleanpath

	def reclaim(self, lease):
		if lease.free_list or lease.size:
			self.log_message("%s", ", ".join(lease.log_fields()))
		lease.reclaim_files()

	def receive_file(self, lease):
		self.reclaim(lease)
		while lease.bytes:
			chunk = self.rfile.read(min(lease.bytes, 2**15))
			if not chunk:
				break
			lease.write(chunk)
			if lease.size != 0 and not lease.bytes:
				with dislock:
					lease.renew()
				self.reclaim(lease)

	def delete_item(self, item):	# with lock
		assert not item.is_root()
		if os.path.exists(item.path) and not os.path.isfile(item.path):
			return None
		if item.is_empty():
			return None
		if not item.is_busy():
			dis.pop(item)
			del disroot.items[item.itemname]
		return item

	def touch_item(self, item):	# with lock
		assert not item.is_root()
		if os.path.exists(item.path) and not os.path.isfile(item.path):
			return None
		if item.is_empty():
			return None
		dis.pop(item)
		dis.insert(item)
		return item

	def oldest_item(self):	# with lock
		if disroot.is_empty():
			return None
		item = disroot.oldest_node()
		return self.touch_item(item)

	def put_lease(self, lease):	# with lock
		try:
			lease.close()
		except Exception as err:
			self.log_message("put_lease() error: %s", repr(err))

	def get_lease_common(self, item):	# with lock
		if os.path.exists(item.path) and not os.path.isfile(item.path):
			return None
		if item.is_busy():
			return None
		try:
			mkdir_p_recursive(disroot.path, os.path.dirname(item.itemname))
		except:
			return None
		try:
			size = int(self.headers["Content-length"])
		except:
			size = None
		return write_lease(item, size)

	def get_lease_append(self, item):	# with lock
		lease = self.get_lease_common(item)
		if lease:
			lease.fd = open(item.path, 'ab')
			lease.renew()
		return lease

	def get_lease_truncate(self, item):	# with lock
		lease = self.get_lease_common(item)
		if lease:
			lease.fd = open(item.path, 'wb')
			lease.renew()
		return lease

	def do_DELETE(self):
		urlpath = self.urlpath()
		if urlpath is None:
			self.send_error(400)
			return
		item = disroot.get_node(urlpath)
		if item.is_root():
			self.respond_badmethod()
			return
		try:
			with dislock:
				item = self.delete_item(item)
		except Exception as err:
			self.send_error(500, repr(err))
			return
		if not item:
			self.send_error(404)
			return
		if not item.is_empty():
			# delete failed, item busy
			self.send_error(409)
			return
		try:
			os.remove(item.path)
		except Exception as err:
			self.send_error(500, repr(err))
			return
		self.respond_success()
		rmdir_p_iterative(item.rootnode.path, item.itemname)

	def do_post_root(self):
		try:
			with dislock:
				item = self.oldest_item()
		except Exception as err:
			self.send_error(500, repr(err))
			return
		if not item or item.is_busy():
			self.send_error(204)
			return
		self.respond_location("/%s" % item.itemname)

	def do_POST(self):
		urlpath = self.urlpath()
		if urlpath is None:
			self.send_error(400)
			return
		item = disroot.get_node(urlpath)
		if item.is_root():
			self.do_post_root()
			return
		try:
			with dislock:
				lease = self.get_lease_append(item)
		except Exception as err:
			self.send_error(500, repr(err))
			return
		if not lease:
			self.send_error(409)
			return
		try:
			self.receive_file(lease)
		except Exception as err:
			self.send_error(500, repr(err))
		else:
			self.respond_success()
		with dislock:
			self.put_lease(lease)

	def do_PUT(self):
		urlpath = self.urlpath()
		if urlpath is None:
			self.send_error(400)
			return
		item = disroot.get_node(urlpath)
		if item.is_root():
			self.respond_badmethod()
			return
		try:
			with dislock:
				lease = self.get_lease_truncate(item)
		except Exception as err:
			self.send_error(500, repr(err))
			return
		if not lease:
			self.send_error(409)
			return
		try:
			self.receive_file(lease)
		except Exception as err:
			self.send_error(500, repr(err))
		else:
			self.respond_success()
		with dislock:
			self.put_lease(lease)

	def do_GET(self):
		urlpath = self.urlpath()
		if urlpath is None:
			self.send_error(400)
			return
		item = disroot.get_node(urlpath)
		if item.is_root():
			self.send_error(204)
			return
		try:
			with dislock:
				item = self.touch_item(item)
		except Exception as err:
			self.send_error(500, repr(err))
			return
		if not item:
			self.send_error(404)
			return
		try:
			f = open(item.path, 'rb')
		except Exception as err:
			self.send_error(500, repr(err))
			return
		self.respond_success(str(os.path.getsize(f.name)), os.stat(f.name).st_mtime)
		self.respond_filedata(f)
		f.close()

	def do_HEAD(self):
		urlpath = self.urlpath()
		if urlpath is None:
			self.send_error(400)
			return
		item = disroot.get_node(urlpath)
		if item.is_root():
			self.send_error(204)
			return
		try:
			with dislock:
				item = self.touch_item(item)
		except Exception as err:
			self.send_error(500, repr(err))
			return
		if not item:
			self.send_error(404)
			return
		self.respond_success(str(os.path.getsize(item.path)), os.stat(item.path).st_mtime)

	def do_OPTIONS(self):
		self.respond_options()

	def send_options(self):
		self.send_response(200)
		self.send_header("Allow", "OPTIONS, HEAD, GET, PUT, POST, DELETE")
		self.send_header("Content-Length", '0')
		if hasattr(self, 'headers') and "Origin" in self.headers:
			self.send_header("Access-Control-Allow-Origin", self.headers["Origin"])
			self.send_header("Access-Control-Allow-Methods", "OPTIONS, HEAD, GET, PUT, POST, DELETE")
			if "Access-Control-Request-Headers" in self.headers:
				self.send_header("Access-Control-Allow-Headers", self.headers["Access-Control-Request-Headers"])
		self.end_headers()

	def send_success(self, lenstr, unixtime=0):
		self.send_response(200)
		self.send_header("Content-Length", lenstr)
		if unixtime:
			self.send_header("Last-Modified", utils.formatdate(unixtime, usegmt=True))
		if hasattr(self, 'headers') and "Origin" in self.headers:
			self.send_header("Access-Control-Allow-Origin", self.headers["Origin"])
		self.end_headers()

	def send_location(self, location):
		self.send_response(200)
		self.send_header("Content-Length", '0')
		self.send_header("Location", location)
		if hasattr(self, 'headers') and "Origin" in self.headers:
			self.send_header("Access-Control-Allow-Origin", self.headers["Origin"])
		self.end_headers()

	def send_badmethod(self):
		self.send_response(405)
		self.send_header("Content-Length", '0')
		self.send_header("Allow", "OPTIONS, HEAD, GET, POST")
		self.end_headers()

	def send_filedata(self, fstream):
		buf = 'not used'
		while buf:
			buf = fstream.read(4096)
			self.wfile.write(buf)

	def respond_options(self):
		try:
			self.send_options()
		except Exception as err:
			self.log_message("respond_options() error: %s", repr(err))

	def respond_success(self, lenstr='0', unixtime=0):
		try:
			self.send_success(lenstr, unixtime)
		except Exception as err:
			self.log_message("respond_success() error: %s", repr(err))

	def respond_location(self, location):
		try:
			self.send_location(location)
		except Exception as err:
			self.log_message("respond_location() error: %s", repr(err))

	def respond_badmethod(self):
		try:
			self.send_badmethod()
		except Exception as err:
			self.log_message("respond_badmethod() error: %s", repr(err))

	def respond_filedata(self, fstream):
		try:
			self.send_filedata(fstream)
		except Exception as err:
			self.log_message("respond_filedata() error: %s", repr(err))

	def log_message(self, format, *args):
		who = str(self.client_address[0])
		if hasattr(self, 'headers') and "X-Forwarded-For" in self.headers:
			who = self.headers["X-Forwarded-For"]
		log_fd.write("%s [%s] %s\n" % (who, self.log_date_time_string(), format % args))
		log_fd.flush()


class dis_server(BaseHTTPServer.HTTPServer, SocketServer.ThreadingMixIn):
	def process_request(self, *args):
		try:
			BaseHTTPServer.HTTPServer.process_request(self, *args)
		except Exception, err:
			log_fd.write("Exception %r\n" % err)
			log_fd.flush()
			raise


if __name__ == "__main__":
	import time
	if len(sys.argv) < 3:
		print "discache.py port /path [%reserve]"
		sys.exit()

	global disreserve
	disreserve = 30
	if len(sys.argv) > 3:
		try:
			disreserve = int(sys.argv[3])
		except:
			disreserve = 0
		if disreserve < 1 or disreserve > 99:
			print "disk reserve percentage (default 30)"
			sys.exit()

	global log_fd
	t = time.strftime("%Y%m%d%H%M%S", time.localtime())
	disk, port = os.path.dirname(os.path.abspath(sys.argv[2])), int(sys.argv[1])
	log_fd = open(os.path.join(disk, "%s-%d.log" % (t, port)), 'a')

	global disroot
	disroot = dis.rootnode(os.path.abspath(sys.argv[2]))
	if disroot.is_empty():
		disroot.write()

	global dislock
	dislock = threading.Lock()

	server = dis_server(('', port), dis_handler)
	server.serve_forever()


