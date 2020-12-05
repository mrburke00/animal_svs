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

# check to see if the topic exists so that we dont try and recreate it
project_path = f'projects/{project_name}'
topic_exists = False

for topic in publisher.list_topics(request={'project': project_path}):
    this_topic_id = str(topic.name).split('/')[-1]

    if this_topic_id == topic_name:
        topic_exists = True
        break

topic_path = publisher.topic_path(project_name, topic_name)

# if topic doesnt exist, create it
if not topic_exists:
    topic = publisher.create_topic(request={"name": topic_path})

# equivalent of 'tail -F <thefile>'
def follow(thefile):
    thefile.seek(0,2)
    while True:
        line = thefile.readline()
        if not line:
            time.sleep(0.1)
            continue
        yield line

# open and follow logs
print('Opened logs file and following new lines...')
f = open(default_log_file)
lines = follow(f)

for i in lines:

    # if its an empty line, skip
    if not len(str(i).strip()):
        continue

    # publish to logs and print
    publisher.publish(topic_path, i.encode('utf-8'))
    print(i)
