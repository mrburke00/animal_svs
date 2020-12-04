import time
import subprocess
import select
import os

default_log_file = os.path.join(os.path.expanduser('~'), 'logs.log')

f = subprocess.Popen(['tail', '-F', default_log_file],\
        stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        
p = select.poll()
p.register(f.stdout)

while True:
    if p.poll(1):
        print f.stdout.readline()
    time.sleep(1)
