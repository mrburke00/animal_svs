# credit to
# https://stackoverflow.com/questions/34953512/replicating-tail-f-with-python
# for logging info

import time
import os
import yaml
import selectors
import subprocess
import sys

from google.cloud import pubsub_v1

#---------------- PubSub code -------------------#

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

#---------------- Start Snakemake and read from stdout and stderr ----------------------#
p = subprocess.Popen(
    ['snakemake', '--cores', '4', '--use-conda', '-F'], stdout=subprocess.PIPE, stderr=subprocess.PIPE
)

sel = selectors.DefaultSelector()
sel.register(p.stdout, selectors.EVENT_READ)
sel.register(p.stderr, selectors.EVENT_READ)

while True:
    for key, _ in sel.select():
        data = key.fileobj.read1().decode()
        if not data:
            exit()

        msg = ''
        if key.fileobj is p.stdout:
            msg = str(data)
        else:
            msg = str(data)

        # publish to logs and print
        publisher.publish(topic_path, msg.encode('utf-8'))
        print(msg)
