import time

default_log_file = os.path.join(os.path.expanduser('~'), 'logs.log')

def follow(thefile):
    thefile.seek(0,2)
    while True:
        line = thefile.readline()
        if not line:
            time.sleep(0.1)
            continue
        yield line

f = open(default_log_file)
lines = follow(f)

for i in lines:
    print (i)
