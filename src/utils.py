import os
import yaml
import gsutils

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


def get_file_from_config(f: str) -> str:
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
        return ''

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


def get_output_dir(output_append: list = []) -> str:
    '''
    Get the output directory for the deployment type

    Inputs:
        output_append:  (list) strings to add as substructure to the output directory. 
                                Input is a list of strings s.t. os.path.join() can be called. Default=[]
    Outputs:
        (str) the directory for the deployment type
    '''

    # join it all first
    output_dir = os.path.join(str(config['run']['output_dir']), *output_append)
    
    if IS_GCP:
        return TO_GS(output_dir)

    return output_dir
        

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
    ref_file = get_file_from_config('reference')

    # get the path either for the symlink or where the copy will go
    moved_ref = os.path.join(str(config['run']['temp_dir']), os.path.basename(ref_file))

    if IS_GCP:
        moved_ref += '.fa'
        return f'gsutil cp gs://{ref_file} {moved_ref} & '

    return f'ln -s {ref_file} {moved_ref} & '

    
