import yaml
import os
import time

import googleapiclient.discovery
import google.auth

# get the google credentials. This should already be setup from the 
# 'gcloud auth login' command. See the README for more details
credentials, project = google.auth.default()
service = googleapiclient.discovery.build('compute', 'v1', credentials=credentials)

# read in the config
config = yaml.safe_load(open('src/config.yaml'))

# variables needed for instance creation
compute_instance_type = str(config['run']['deployment']['gcp_instance']['machine_type'])
disk_space = str(config['run']['deployment']['gcp_instance']['disk_space'])
ram = str(config['run']['deployment']['gcp_instance']['ram_size'])

project_name = str(config['run']['deployment']['project_name'])

region = str(config['run']['deployment']['gcp_instance']['region'])
zone = str(config['run']['deployment']['gcp_instance']['zone'])

# we choose ubuntu
vm_family = 'ubuntu-1604-lts'
vm_project = 'ubuntu-os-cloud'


def create_instance(compute, project, zone, name):
    '''Create a compute instance in GCP in a project in a zone with the 
    given name. 

    Inputs:
        compute:    the compute instance to setup. This is a google dict like 
                    object used for REST API calls. See the link for more. 
                    https://googleapis.github.io/google-api-python-client/docs/epy/googleapiclient.discovery-module.html#build
        project:    string. The name of the project
        zone:       string. The zone where the compute is to be setup 
        name:       string. The name to give the compute instance

    Returns: 
        Response body (dictionary). Information on the compute instance setup. 
        See the link for more info. 
        https://cloud.google.com/compute/docs/reference/rest/v1/instances/insert
    '''
    global compute_instance_type, vm_project, vm_family

    # Get the ubuntu image
    image_response = compute.images().getFromFamily(
        project=vm_project, family=vm_family).execute()
    source_disk_image = image_response['selfLink']

    # Configure the machine
    machine_type = f"zones/{zone}/machineTypes/{compute_instance_type}"
    startup_script = open(
        os.path.join(
            os.path.dirname(__file__), 'setup.sh'), 'r').read()

    config = {
        'name': name,
        'machineType': machine_type,

        # Specify the boot disk and the image to use as a source.
        'disks': [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image,
                }
            }
        ],

        # Specify a network interface with NAT to access the public
        # internet.
        'networkInterfaces': [{
            'network': 'global/networks/default',
            'accessConfigs': [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
            ]
        }],

        # Allow the instance to access cloud storage and logging.
        'serviceAccounts': [{
            'email': 'default',
            'scopes': [
                'https://www.googleapis.com/auth/devstorage.read_write',
                'https://www.googleapis.com/auth/pubsub',
                'https://www.googleapis.com/auth/logging.write'
            ]
        }],

        # Metadata is readable from the instance and allows you to
        # pass configuration from deployment scripts to instances.
        'metadata': {
            'items': [{
                # Startup script is automatically executed by the
                # instance upon startup.
                'key': 'startup-script',
                'value': startup_script
            }]
        }
    }

    return compute.instances().insert(
        project=project,
        zone=zone,
        body=config).execute()

def delete_instance(compute, project, zone, name):
    '''Delete the comput instance specified

    Inputs:
        compute:    the compute instance to setup. This is a google dict like 
                    object used for REST API calls. See the link for more. 
                    https://googleapis.github.io/google-api-python-client/docs/epy/googleapiclient.discovery-module.html#build
        project:    string. The name of the project
        zone:       string. The zone where the compute is to be setup 
        name:       string. The name to give the compute instance

    Returns: 
        Response body (dictionary). Information on the compute instance setup. 
        See the link for more info. 
        https://cloud.google.com/compute/docs/reference/rest/v1/instances/insert
    '''
    return compute.instances().delete(
        project=project,
        zone=zone,
        instance=name).execute()


def wait_for_operation(compute, project, zone, operation):
    '''Block until the operation (specified by the response body of a request)
    is finished. Raises an exception if it fails (specified by a google response)

    Inputs:
        compute:    the compute instance to setup. This is a google dict like 
                    object used for REST API calls. See the link for more. 
                    https://googleapis.github.io/google-api-python-client/docs/epy/googleapiclient.discovery-module.html#build
        project:    string. The name of the project
        zone:       string. The zone where the compute is to be setup 
        operation:  dict-like. the repsonse body from some operation (like a 
                    setup or teardown or insert)

    Returns: 
        Response body (dictionary). Information on the compute instance setup. 
        See the link for more info. 
        https://cloud.google.com/compute/docs/reference/rest/v1/instances/insert
    '''
    print('Waiting for operation to finish...')
    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
            operation=operation).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)


def setup(project=project_name, zone=zone, instance_name='vc-pipeline'):
    '''Create a GCP compute instance and block until the operation is complete. 
    
    Inputs:
        project_name:   string. The name of the GCP project. Default is pulled 
                        from the config.yaml file
        zone:           string. The zone where the GCP compute instance is to 
                        be created. Default is from the config.yaml file
        instance_name:  string. The name to give the compute VM instance. 
                        Default is 'vc-pipeline'

    Returns: 
        None
    '''

    compute = googleapiclient.discovery.build('compute', 'v1')

    #---------------- Set up the instance -----------------------#

    print('Creating instance...')

    operation = create_instance(compute, project, zone, instance_name)
    wait_for_operation(compute, project, zone, operation['name'])

    print('Done')


def teardown(project=project_name, zone=zone, instance_name='vc-pipeline'):
    '''Delete a GCP compute instance and block until the operation is complete

    Inputs:
        project_name:   string. The name of the GCP project. Default is pulled 
                        from the config.yaml file
        zone:           string. The zone where the GCP compute instance is to 
                        be created. Default is from the config.yaml file
        instance_name:  string. The name to give the compute VM instance. 
                        Default is 'vc-pipeline'

    Returns: 
        None
    '''

    compute = googleapiclient.discovery.build('compute', 'v1')

    print('Deleting instance...')

    operation = delete_instance(compute, project, zone, instance_name)
    wait_for_operation(compute, project, zone, operation['name'])

    print('Done')
