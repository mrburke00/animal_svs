# credit to
# https://stackoverflow.com/questions/34953512/replicating-tail-f-with-python
# for logging info

import time
import os
import yaml

from google.cloud import pubsub_v1

# google cloud log topic
topic_name = 'logs'

# open the config for the poject name
config = yaml.safe_load(open('config.yaml'))
project_name = str(config['run']['deployment']['project_name'])

# file where stdout and sterr are redirected
default_log_file = os.path.join('logs.log')

# ability to publish to google pub sub for logs
publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(project_name, topic_name)
topic = publisher.create_topic(request={"name": topic_path})

def follow(thefile):
    thefile.seek(0,2)
    while True:
        line = thefile.readline()
        if not line:
            time.sleep(0.1)
            continue
        yield line

# open and follow logs
f = open(default_log_file)
lines = follow(f)

for i in lines:

    # if its an empty line, skip
    if not len(str(i).strip()):
        continue

    # publish to logs and print
    publisher.publish(topic_name, i)
    print(i)
