import os
import sys

import dis

if __name__ == "__main__":
	if len(sys.argv) < 3:
		print "disrepair.py rootpath [insert|pop|touch] itemname"
		sys.exit()
	rootpath, cmd, itemname = sys.argv[1:]
	root = dis.rootitem(os.path.abspath(rootpath))
	item = root.get_node(itemname)
	if cmd == "pop":
		item.pop()
	if cmd == "insert":
		if not item.is_empty():
			print "smashing current pointers [%s : %s]" % (item.prev, item.next)
		item.prev = item.next = item.itemname
		item.insert()
	if cmd == "touch":
		item.pop()
		item.insert()


