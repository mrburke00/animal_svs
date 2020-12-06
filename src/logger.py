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

# Google cloud pubsub variables
topic_name = 'logs'
sub_name = 'logs-consumer'


class Logger:
    '''Logs the output to the correct source based on the config'''

    def __init__(self, role: str ='publisher'):
        '''
        Initialize a logger.

        Inputs:
            role: (str) the role of this logger. Either {'publisher', 'consumer'}. Default='publisher'
        '''
        role = 'publisher' if 'consumer' not in role else 'consumer'

        self.config = yaml.safe_load(open('config.yaml'))

        self.IS_CLOUD = str(self.config['run']['deployment']['type']).lower() == 'cloud'
        self.IS_GCP = self.IS_CLOUD and str(self.config['run']['deployment']['service']).lower() == 'gcp'

        # extract the project name if IS_GCP
        if self.IS_GCP:
            self.project_name = str(self.config['run']['deployment']['project_name'])

        if role == 'publisher':
            self.publish_func = self.__init_publisher()

        else:
            self.__init_consumer()

    def __init_publisher(self) -> callable:
        '''
        Create a publisher function that publishes based on the config
        '''
        publish_func = None

        if self.IS_GCP:

            print('Setting up connection to google pubsub...')

            #---------------- PubSub code -------------------#

            # ability to publish to google pub sub for logs
            publisher = pubsub_v1.PublisherClient()

            # check to see if the topic exists so that we dont try and recreate it
            project_path = f'projects/{self.project_name}'
            topic_exists = False

            for topic in publisher.list_topics(request={'project': project_path}):
                this_topic_id = str(topic.name).split('/')[-1]

                if this_topic_id == topic_name:
                    topic_exists = True
                    break

            topic_path = publisher.topic_path(self.project_name, topic_name)

            # if topic doesnt exist, create it
            if not topic_exists:
                topic = publisher.create_topic(request={"name": topic_path})

            def publish(msg: str):
                if not len(msg.strip()):
                    return
                    
                publisher.publish(topic_path, msg.encode('utf-8'))
                print(msg)

            publish_func = publish

            print('Done.')

        else:
            publish_func = lambda msg: print(msg)


        return publish_func

    def __init_consumer(self) -> None:
        '''
        Initialize consumer. Only used for remote logging
        '''

        # if gcp, we need to subscribe
        if self.IS_GCP:

            print('Setting up connection to google pubsub...')

            # check to see if the topic exists
            topic_exists = False

            publisher = pubsub_v1.PublisherClient()
            project_path = f'projects/{self.project_name}'

            for topic in publisher.list_topics(request={'project': project_path}):
                this_topic_id = str(topic.name).split('/')[-1]

                if this_topic_id == topic_name:
                    topic_exists = True
                    break

            # if we don't have the topic, no reason to listen so break
            if not topic_exists:
                print(f'Topic {topic_name} does not exist. Exiting...')
                raise NameError(f'Topic {topic_name} not found')

            # setup the subscriber
            subscriber = pubsub_v1.SubscriberClient()
            topic_path = subscriber.topic_path(self.project_name, topic_name)
            subscription_path = subscriber.subscription_path(self.project_name, sub_name)

            subsription = None

            self.subscriber = subscriber

            # check to see if the subscription exists
            sub_exists = False

            for sub in subscriber.list_subscriptions(request={"project": project_path}):
                this_sub_id = str(sub.name).split('/')[-1]

                if this_sub_id == sub_name:
                    sub_exists = True
                    break

            # if the sub does not exist, create it
            if not sub_exists:
                subscription = subscriber.create_subscription(
                    request={"name": subscription_path, "topic": topic_path}
                )

            # get it otherwise
            else:
                subscription = subscriber.get_subscription(
                    request={"subscription": subscription_path}
                )

            print('Done')


    def log(self, msg: str) -> None:
        '''
        Log a message
        '''
        self.publish_func(msg)

    def consume(self):
        '''
        Start consuming log messages
        '''
        if self.IS_GCP:
            subscription_path = self.subscriber.subscription_path(self.project_name, sub_name)

            # Wrap the subscriber in a 'with' block to automatically call close() to
            # close the underlying gRPC channel when done.
            with self.subscriber:

                # callback is just to print data to console.
                def callback(message):
                    print(message.data.decode('utf-8'))
                    message.ack()

                # now wait
                future = self.subscriber.subscribe(subscription_path, callback)

                print('Waiting for log messages from pipeline...')

                try:
                    future.result()
                except KeyboardInterrupt:
                    future.cancel()
