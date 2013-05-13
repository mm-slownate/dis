import os
import sys
import BaseHTTPServer
import SocketServer
import urllib
import threading

from urlparse import urlparse

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
		return (float(fsstat.f_bavail) / fsstat.f_blocks)*100

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
		assert self.bytes
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
		yield "%dKB left" % (self.size/2**10)
		yield "%s%% reserve" % self.rsv_str()
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


class skel_handler(BaseHTTPServer.BaseHTTPRequestHandler):
	def prefetch(self):
		path = urllib.unquote(urlparse(self.path)[2]).lstrip('/')
		if not path:
			return disroot
		return disroot.get_node(path)

	def fetch_and_delete(self):	# with lock
		item = self.prefetch()
		if not item.is_root():
			if item.is_empty():
				return None
			if not item.is_busy():
				dis.pop(item)
				del disroot.items[item.itemname]
		return item

	def fetch(self):	# with lock
		item = self.prefetch()
		if not item.is_root():
			if item.is_empty():
				return None
			dis.pop(item)
			dis.insert(item)
		return item

	def get_lease_append(self, item):	# with lock
		if item.is_busy():
			return None
		size = None
		if "Content-length" in self.headers:
			size = int(self.headers["Content-length"])
		lease = write_lease(item, size)
		mkdir_p_recursive(disroot.path, os.path.dirname(item.itemname))
		lease.fd = open(item.path, 'ab')
		lease.renew()
		return lease

	def get_lease_truncate(self, item):	# with lock
		if item.is_busy():
			return None
		mkdir_p_recursive(disroot.path, os.path.dirname(item.itemname))
		size = None
		if "Content-length" in self.headers:
			size = int(self.headers["Content-length"])
		lease = write_lease(item, size)
		lease.fd = open(item.path, 'wb')
		lease.renew()
		return lease

	def do_DELETE(self):
		with dislock:
			item = self.fetch_and_delete()
		if item is None:
			self.send_response(404)
			return
		if item.is_root():
			self.send_response(405)
			return
		if not item.is_empty():
			# delete failed, item busy, return "locked"
			self.send_response(423)
			return
		os.remove(item.path)
		r = item.rootnode.path
		n = os.path.dirname(item.itemname)
		while n:
			try:
				os.rmdir('/'.join([r, n]))
			except:
				break
			n = os.path.dirname(n)
		self.send_response(200)

	def do_POST(self):
		item = self.prefetch()
		if item.is_root():
			self.send_response(405)
			return
		with dislock:
			lease = self.get_lease_append(item)
		if not lease:
			self.send_response(423)
			return
		self.log_message("%s", ", ".join(lease.log_fields()))
		lease.reclaim_files()
		while lease.bytes:
			chunk = self.rfile.read(min(lease.bytes, 2**15))
			if not chunk:
				break
			lease.write(chunk)
			if lease.size != 0 and not lease.bytes:
				with dislock:
					lease.renew()
				self.log_message("%s", ", ".join(lease.log_fields()))
				lease.reclaim_files()
		with dislock:
			lease.close()
		self.send_response(200)

	def do_PUT(self):
		item = self.prefetch()
		if item.is_root():
			self.send_response(405)
			return
		with dislock:
			lease = self.get_lease_truncate(item)
		if not lease:
			self.send_response(423)
			return
		self.log_message("%s", ", ".join(lease.log_fields()))
		lease.reclaim_files()
		while lease.bytes:
			chunk = self.rfile.read(min(lease.bytes, 2**15))
			if not chunk:
				break
			lease.write(chunk)
			if lease.size != 0 and not lease.bytes:
				with dislock:
					lease.renew()
				self.log_message("%s", ", ".join(lease.log_fields()))
				lease.reclaim_files()
		with dislock:
			lease.close()
		self.send_response(200)

	def do_GET(self):
		with dislock:
			item = self.fetch()
		if item is None or item.is_root():
			self.send_response(404)
			return
		f = open(item.path, 'rb')
		self.send_response(200)
		self.send_header("Content-Length", os.path.getsize(f.name))
		self.end_headers()
		buf = 'not used'
		while buf:
			buf = f.read(4096)
			self.wfile.write(buf)
		f.close()

	def do_HEAD(self):
		with dislock:
			item = self.fetch()
		if item is None or item.is_root():
			self.send_response(404)
			return
		self.send_response(200)
		self.send_header("Content-Length", os.path.getsize(item.path))

	def do_OPTIONS(self):
		self.send_response(200)
		self.send_header("Allow", "OPTIONS, HEAD, GET, PUT, POST, DELETE")
		self.send_header("Content-Length", '0')
		self.end_headers()

	def log_message(self, format, *args):
		log_fd.write("%s [%s] %s\n" % (self.address_string(), self.log_date_time_string(), format % args))
		log_fd.flush()


class skel_server(BaseHTTPServer.HTTPServer, SocketServer.ThreadingMixIn):
	def process_request(self, *args):
		try:
			BaseHTTPServer.HTTPServer.process_request(self, *args)
		except Exception, err:
			log_fd.write("Exception %s\n" % err)
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

	server = skel_server(('', port), skel_handler)
	server.serve_forever()


