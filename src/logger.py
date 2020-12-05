import time
import subprocess
import select
import os

default_log_file = os.path.join(os.path.expanduser('~'), 'logs.log')

with open(default_log_file, 'r') as f:
    time.sleep(1)
    print(f.readline())
