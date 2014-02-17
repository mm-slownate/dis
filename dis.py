import os
import sys
import xattr

class node:
	def __init__(self, rootnode, itemname):
		self.rootnode = rootnode
		self.itemname = itemname
		self.prev = self.next = itemname
		if rootnode is not self:
			self.path = '/'.join([rootnode.path, itemname])
		assert not itemname in rootnode.items
		try:
			self.__read()
		except:
			pass
		rootnode.items[itemname] = self

	def is_valid(self):
		return not ((self.prev == self.itemname) ^ (self.next == self.itemname))

	def is_empty(self):
		assert self.is_valid()
		return (self.prev == self.itemname)

	def is_pair(self):
		assert self.is_valid()
		return (not self.is_empty()) and (self.prev == self.next)

	def is_root(self):
		return self.rootnode is self

	def is_busy(self):
		return self in self.rootnode.leases

	def get_prev(self):
		prev = self.prev
		if prev in self.rootnode.items:
			return self.rootnode.items[prev]
		assert prev
		return node(self.rootnode, prev)

	def get_next(self):
		next = self.next
		if next in self.rootnode.items:
			return self.rootnode.items[next]
		assert next
		return node(self.rootnode, next)

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


class rootnode(node):
	def __init__(self, path):
		self.path = path
		self.items = {}
		self.leases = []
		node.__init__(self, self, '')

	def get_node(self, itemname):
		if itemname in self.items:
			return self.items[itemname]
		return node(self, itemname)

	def oldest_node(self):
		return self.get_node(self.prev)


def pop(item):
	assert not item.is_empty()

	p, n = item.get_prev(), item.get_next()
	p.next = n.itemname
	n.prev = p.itemname
	p.write()
	n.write()

	item.prev = item.next = item.itemname
	item.write()


def insert(item):
	assert item.is_empty()

	p, n = item.rootnode, item.rootnode.get_next()
	p.next = n.prev = item.itemname
	n.write()
	p.write()

	item.prev = p.itemname
	item.next = n.itemname
	item.write()


def file_exists_in_cache(item):		# with lock
	assert not item.is_root()
	if not os.path.exists(item.path):
		return False
	if not os.path.isfile(item.path):
		return False
	if item.is_empty():
		return False
	return True


def delete_item(item):		# with lock
	if not file_exists_in_cache(item):
		return None
	if not item.is_busy():
		pop(item)
		del item.rootnode.items[item.itemname]
	return item


def touch_item(item):		# with lock
	if not file_exists_in_cache(item):
		return None
	pop(item)
	insert(item)
	return item


def do_init(path):
	root = rootnode(os.path.abspath(path))
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
	root = rootnode(os.path.abspath(sys.argv[1]))
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


