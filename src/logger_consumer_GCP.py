import os
import yaml

from google.cloud import pubsub_v1

# google cloud log topic
topic_name = 'logs'

sub_name = 'logs_consumer'

# open the config for the poject name
config = yaml.safe_load(open('config.yaml'))
project_name = str(config['run']['deployment']['project_name'])

subscriber = pubsub_v1.SubscriberClient()
topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id=project_name,
    topic=topic_name,
)
subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
    project_id=project_name,
    sub=sub_name,  # Set this to something appropriate.
)
subscriber.create_subscription(
    name=subscription_name, topic=topic_name
)

def callback(message):
    print(message.data)
    message.ack()

future = subscriber.subscribe(subscription_name, callback)
