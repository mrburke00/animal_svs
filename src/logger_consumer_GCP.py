import os
import yaml

from google.cloud import pubsub_v1 as pubsub

# google cloud log topic
topic_name = 'logs'

sub_name = 'logs-consumer'

# open the config for the poject name
config = yaml.safe_load(open('config.yaml'))
project_name = str(config['run']['deployment']['project_name'])

# check to see if the topic exists
topic_exists = False

publisher = pubsub.PublisherClient()
project_path = f'projects/{project_name}'

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
subscriber = pubsub.SubscriberClient()
topic_path = subscriber.topic_path(project_name, topic_name)
subscription_path = subscriber.subscription_path(project_name, sub_name)

subsription = None

# Wrap the subscriber in a 'with' block to automatically call close() to
# close the underlying gRPC channel when done.
with subscriber:

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

    # callback is just to print data to console.
    def callback(message):
        print(message.data.decode('utf-8'))
        message.ack()

    # now wait
    future = subscriber.subscribe(subscription_path, callback)

    print('Waiting for log messages from pipeline...')

    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()
