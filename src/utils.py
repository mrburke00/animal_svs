import os
import yaml

from snakemake.remote.GS import RemoteProvider as GSRemoteProvider

# load the config
config = yaml.safe_load(open('./config.yaml'))

# some high level variables to avoid re-indexing the yaml
IS_CLOUD = str(config['run']['deployment']['type']).lower() == 'cloud'
IS_GCP = str(config['run']['deployment']['service']).lower() == 'gcp' and IS_CLOUD

BUCKET_NAME = str(config['run']['deployment']['bucket_name'])

# setup GS incase we use cloud storage
GS = None 
TO_GS = None

if IS_GCP:
    GS = GSRemoteProvider()
    TO_GS = lambda f: GS.remote(str(config['run']['deployment']['bucket_name']) + '/' + f)


def get_sample_name() -> str:
    '''
    Get the sample name for string templating

    Inputs:
        None
    Outputs:
        (str)
    '''
    if str(config['input']['sample_name']) == '':
        return os.path.basename(config['input']['forward'])

    return str(config['input']['sample_name'])


def get_file(f: str) -> str:
    '''
    Get the specified file from the config for the deployment type. 
    Files available are {'reference', 'forward', 'reverse'}

    Inputs:
        f:      (str) which file to get
    Outputs:
        (str) file name for deployment type
    '''
    # the files allowed to retreive
    allowed_files = {'reference', 'forward', 'reverse'}

    # keep lower for normalization
    f = f.lower()

    if f not in allowed_files:
        raise FileNotFoundError(f'Could not find file {f} in config.')

    # separate logic for GCP 
    if IS_GCP:
        
        if f == 'reference':
            return TO_GS(str(config['input'][f]))

        else:
            return TO_GS(str(config['input']['samples'][f]))

    if f == 'reference':
        return str(config['input'][f])

    else:
        return str(config['input']['samples'][f])


def get_dir(directory: str, dir_append: list = []) -> str:
    '''
    Get the directory for the deployment type

    Inputs:
        directory:  (str) the directory to get {'temp', 'logs', 'output'}
        dir_append: (list) strings to add as substructure to the directory. 
                                Input is a list of strings s.t. os.path.join() can be called. Default=[]
    Outputs:
        (str) the directory for the deployment type
    '''

    # the directories allowed
    allowed_dirs = {'temp', 'logs', 'output'}

    directory = directory.lower()

    if directory not in allowed_dirs:
        raise NameError(f'Directory of type {directory} not supported.')

    # get the actual name
    if directory == 'temp':
        directory = 'temp_dir'
    
    elif directory == 'logs':
        directory = 'logs_dir'

    else:
        directory = 'output_dir'

    # join the full path
    full_dir = os.path.join(str(config['run'][directory]), *dir_append)
    
    # as of right now, we only support output directories in GCP
    if IS_GCP and 'output' in directory:
        return TO_GS(full_dir)

    return full_dir
        

def move_ref_to_temp_cmd() -> str:
    '''
    Some steps in the pipeline require that the .fa and .fai be in the same directory. 
    Due to this, we must either copy the .fa to the temp directory or add a symlink.
    This depends on the deployment type (GCP does not allow for symlinks), so 
    the command will be generated here. 

    Inputs:
        None
    Outputs:
        (str) the command to execute
    ''' 
    # get the reference file
    ref_file = get_file('reference')

    # get the path either for the symlink or where the copy will go
    moved_ref = os.path.join(str(config['run']['temp_dir']), os.path.basename(ref_file))

    if IS_GCP:
        moved_ref += '.fa'
        return f'gsutil cp gs://{ref_file} {moved_ref} & '

    return f'ln -s {ref_file} {moved_ref} & '

    
