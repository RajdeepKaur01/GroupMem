#!/usr/bin/env python
import sys
if len(sys.argv) < 3:
	print "Usage: %s <file_name> <file_size(MB)>" % sys.argv[0]
        exit()
file = open(sys.argv[1], "w")
str = 'a'
for i in range(20):
	str = str + str;
for i in range(int(sys.argv[2])):
	file.write(str)
