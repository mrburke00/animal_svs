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

# google cloud log topic
topic_name = 'logs'

class Logger:
    '''Logs the output to the correct source based on the config'''
    def __init__(self):
        self.config = yaml.safe_load(open('config.yaml'))
        self.IS_CLOUD = str(self.config['run']['deployment']['type']).lower() == 'cloud'
        self.IS_GCP = self.IS_CLOUD and str(self.config['run']['deployment']['service']).lower() == 'gcp'

        self.publish_func = self.__init_publisher()

    def __init_publisher(self) -> callable:
        '''
        Create a publisher function that publishes based on the config
        '''
        publish_func = None

        if self.IS_GCP:

            print('Setting up connection to google pubsub...')

            #---------------- PubSub code -------------------#
            project_name = str(self.config['run']['deployment']['project_name'])

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

            def publish(msg: str):
                publisher.publish(topic_path, msg.encode('utf-8'))
                print(msg)
            
            publish_func = publish

            print('Done.')

        else:
            publish_func = lambda msg: print(msg)


        return publish_func

    def log(self, msg: str) -> None:
        '''
        Log a message
        '''
        self.publish_func(msg)
