#!/usr/bin/env python3
import sys
from subprocess import Popen, PIPE, STDOUT

with Popen("command", stdout=PIPE, stderr=STDOUT, bufsize=1) as p, \
     open('logfile', 'ab') as file:
    for line in p.stdout: # b'\n'-separated lines
        sys.stdout.buffer.write(line) # pass bytes as is
        file.write(line)
