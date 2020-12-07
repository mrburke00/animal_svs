import yaml
import subprocess
import gcp_setup_instance
import sys

from src import logger

# import the config
config = yaml.safe_load(open('src/config.yaml'))

IS_CLOUD = str(config['run']['deployment']['type']).lower() == 'cloud'
IS_GCP = IS_CLOUD and str(config['run']['deployment']['service']).lower() == 'gcp'

# pipeline done message from remote
pipeline_complete_msg = 'Pipeline run complete'

# some trickery as GCP
if IS_GCP:

    # since we can pass a startup script to GCP, lets edit setup to also pass in and
    # save our config file
    startup = ''

    with open('setup_template.sh', 'r') as o:
        for line in o:
            startup += f'{line}'

    # now we want to add the config to the startup script
    config_yaml = ''

    with open('src/config.yaml', 'r') as o:
        for line in o:
            config_yaml += line

    # now append config to startup
    startup += f'echo "{config_yaml}" > $HOME/animal_svs/src/config.yaml'

    # now add change into src and run run.py
    startup += '\ncd $HOME/animal_svs/src\npython run.py'

    # now save this as setup.sh
    with open('setup.sh', 'w') as o:
        o.write(startup)


    # then we launch an instance
    gcp_setup_instance.setup()

    # define the callback for GCP
    def cb(message):
        print(message.data.decode('utf-8'))
        message.ack()

        # if the message is the end message, kill the remote
        if pipeline_complete_msg in message.data.decode('utf-8'):
            gcp_setup_instance.teardown()
            print('Exiting...')
            sys.exit(0)
        

    # define the function to run on keyboard interrupt inside the logger
    def ki():
        print('\nKeyboardInterrupt. Killing remote process')
        print('DO NOT PERFORM ANOTHER KEYBOARD INTERRUPT')
        print('If you do, you may need to manually delete the VM instance from your GCP Compute Engine')
        print('Beginning teardown...')
        gcp_setup_instance.teardown()
        print('Exiting...')
        sys.exit(1)

    # start up the logger
    l = logger.Logger('consumer')
    l.consume(callback=cb, keyboard_interrupt=ki)

# otherwise just run run.py
else:
    subprocess.run(['python', 'src/run.py'])
