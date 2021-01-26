from snakemake.remote.GS import RemoteProvider as GSRemoteProvider
from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider

class Service: 

    def __init__(self):
        raise NotImplementedError()

    def validate_files(self, files: list): 
        '''Validate that files exist

        :param files list: strings of the full path to files (for cloud based, must include prefix)

        :raises ValueError: if file not found
        '''
        raise NotImplementedError() 

    def file_connection(self, file_name: str): 
        raise NotImplementedError()


class AWSService(Service):

    def __init__(self): 
        '''Create a channel for connecting to S3 remote files
        '''

        # NOTE: we are assuming that 'aws configure' has been run with AWS CLI
        # setup since we don't want plain text keys. 
        # Docs: https://snakemake.readthedocs.io/en/stable/snakefiles/remote_files.html
        self.s3 = S3RemoteProvider()


class GCPService(Service): 

    def __init__(self): 
        ''' Create a channel for connecting to GCP remote files
        '''

        # NOTE: we are assuming that 'gcloud auth application-default login' has been run with GCP CLI
        # setup since we don't want plain text keys. 
        # Docs: https://snakemake.readthedocs.io/en/stable/snakefiles/remote_files.html
        self.gs = GSRemoteProvider()

class LocalService(Service):

    def __init__(self): 
        pass
