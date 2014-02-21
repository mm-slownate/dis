import os
import sys
import xattr

class node:
	def __init__(self, path, itemname=''):
		self.path = path
		self.itemname = itemname
		if itemname:
			self.path = '/'.join([path, itemname])
		try:
			self.__read()
		except:
			self.prev = self.next = itemname

	def is_valid(self):
		return not ((self.prev == self.itemname) ^ (self.next == self.itemname))

	def is_empty(self):
		assert self.is_valid()
		return (self.prev == self.itemname)

	def __read(self):
		raw = xattr.getxattr(self.path, "user.dis")
		t = raw.split(':')
		if t[0] != "dis-file-list" or len(t) != 3:
			raise RuntimeError("%s: incorrect attr format" % self.itemname)
		self.prev, self.next = t[1:]
		assert self.is_valid()

	def write(self):
		assert self.is_valid()
		if ':' in self.next or ':' in self.prev:
			raise RuntimeError("%s: unescaped ':' in link")
		out = ':'.join(["dis-file-list", self.prev, self.next])
		xattr.setxattr(self.path, "user.dis", out)


class rootitem(node):
	def __init__(self, path):
		node.__init__(self, path)
		self.items = {'': self}
		self.leases = []

	def is_root(self):
		return True

	def get_node(self, itemname):
		if itemname not in self.items:
			assert itemname
			self.items[itemname] = item(self, itemname)
		return self.items[itemname]

	def get_prev(self):
		if self.is_empty():
			return None
		return self.get_node(self.prev)

	def get_next(self):
		if self.is_empty():
			return None
		return self.get_node(self.next)

	def oldest_node(self):
		return self.get_prev()


class item(node):
	def __init__(self, rootnode, itemname):
		node.__init__(self, rootnode.path, itemname)
		self.rootnode = rootnode

	def is_root(self):
		return False

	def is_busy(self):
		return self in self.rootnode.leases

	def take_lease(self):
		self.rootnode.leases.append(self)

	def drop_lease(self):
		self.rootnode.leases.remove(self)

	def get_prev(self):
		return self.rootnode.get_node(self.prev)

	def get_next(self):
		return self.rootnode.get_node(self.next)

	def pop(self):
		assert self.file_exists_in_cache()

		p, n = self.get_prev(), self.get_next()
		p.next = n.itemname
		n.prev = p.itemname
		p.write()
		n.write()

		self.prev = self.next = self.itemname
		self.write()

	def insert(self):
		assert self.is_empty()

		p, n = self.rootnode, self.rootnode.get_next()
		p.next = n.prev = self.itemname
		n.write()
		p.write()

		self.prev = p.itemname
		self.next = n.itemname
		self.write()

	def file_exists_in_cache(self):
		if not os.path.exists(self.path):
			return False
		if not os.path.isfile(self.path):
			return False
		if self.is_empty():
			return False
		return True

	def delete(self):
		if not self.file_exists_in_cache():
			return None
		if not self.is_busy():
			self.pop()
			del self.rootnode.items[self.itemname]
		return self

	def touch(self):
		if not self.file_exists_in_cache():
			return None
		self.pop()
		self.insert()
		return self


def do_init(path):
	root = rootitem(os.path.abspath(path))
	if not root.is_empty():
		print "smashing current pointers [%s : %s]" % (root.prev, root.next)
	print "init dis %s" % root.path
	root.prev = root.next = root.itemname
	root.write()


if __name__ == "__main__":
	if len(sys.argv) < 2:
		print "dis.py [init] path"
		sys.exit()
	if len(sys.argv) > 2:
		if sys.argv[1] != "init":
			print "dis.py [init] path"
			sys.exit()
		do_init(sys.argv[2])
		sys.exit()
	root = rootitem(os.path.abspath(sys.argv[1]))
	if root.is_empty():
		print "no files"
		sys.exit()
	visited = []
	n = root
	while n.next != '':
		if n.next == n.itemname:
			raise RuntimeError("corrupt file list")
		prev = n.itemname
		n = n.get_next()
		print n.itemname
		if n.itemname in visited:
			print "!!! duplicate item"
			sys.exit()
		visited.append(n.itemname)
		if n.prev != prev:
			print "!!! incorrect prev pointer %s %s" % (n.prev, prev)
			sys.exit()


