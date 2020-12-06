import yaml
import subprocess
import gcp_setup_instance

from src import logger

# import the config
config = yaml.safe_load(open('src/config.yaml'))

IS_CLOUD = str(config['run']['deployment']['type']).lower() == 'cloud'
IS_GCP = IS_CLOUD and str(config['run']['deployment']['service']).lower() == 'gcp'

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
    startup += f'echo "{config_yaml}" > $HOME/animal_svs/rc/config.yaml'

    # now add change into src and run run.py
    startup += '\ncd $HOME/animal_svs/src\npython run.py'

    # now save this as setup.sh
    with open('setup.sh', 'w') as o:
        o.write(startup)

    # then we launch an instance
    gcp_setup_instance.setup()

    # start up the logger
    l = logger.Logger('consumer')
    l.consume()

    # now we need to wait for the pipeline to end the shut it down
    # TODO: wait
    # gcp_setup_instance.teardown()

# otherwise just run run.py
else:
    subprocess.run(['python', 'src/run.py'])
