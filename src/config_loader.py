import os
import yaml
import pathlib

from service import Service

from snakemake.remote.GS import RemoteProvider as GSRemoteProvider

# keys for accessing parts of the config
INPUT_KEY = 'input'
MANIFEST_KEY = 'manifest'
REFERENCE_KEY = 'reference'
ARGS_KEY = 'run'
TEMP_DIR_KEY = 'temp_dir'
LOG_DIR_KEY = 'logs_dir'
OUTPUT_DIR_KEY = 'output_dir'

# extensions valid for certain file types
VALID_REF_EXTS = {'fa', 'fasta'}
VALID_FASTQ_EXTS = {'fq', 'fastq', 'fq.gz', 'fastq.gz'}
VALID_BAM_EXTS = {'bam'}
VALID_INDEXED_BAM_EXTS = {'bai'}

class Config: 

    ################# Private #################

    def __init__(self, config_file: str, service: Service):

        self.service = service
        self.config_dict = yaml.safe_load(open(config_file))
        self._validate_config()

    def _validate_config(self):
        '''Make sure all required fields of the config are valid
        '''

        self._validate_manifest()
        self._validate_reference()
        self._validate_dirs()

    def _validate_manifest(self):
        '''Make sure all forward fastqs have a reverse counterpart and bam
        files have indexed bams
        '''

        manifest_file = self.config_dict.get(INPUT_KEY, {}).get(MANIFEST_KEY, '')

        if manifest_file == '' or not pathlib.Path(manifest_file):
            raise ValueError('Manifest file not found')
        
        # go through each line and make sure that pairs have either .fq .fastq
        # or .bam if alone
        inputs = []

        with open(manifest_file, 'r') as f:

            for line in f: 

                inputs.append(line.split(','))

        # make sure all inputs come as pairs
        if not all([len(x) == 2 for x in inputs]):
            raise ValueError('''
            Manifest file must be of one of the following forms: 
            1. Fastq inputs: 
                sample_1.fq,sample_2.fq

            2. Bam inputs:
                sample.bam,sample.bai
            ''')

        # determine if its fastqs or bams
        if pathlib.PurePath.suffix(inputs[0][0]) in VALID_FASTQ_EXTS:

            if not all([
                pathlib.PurePath.suffix(x[0]) in VALID_FASTQ_EXTS 
                and pathlib.PurePath.suffix(x[1]) in VALID_FASTQ_EXTS 
                for x in inputs
            ]):
                raise ValueError('''
            Error with fastq files. Input fastq files should end in ".fq" or ".fastq". 
            Forward and reverse should be separated by a comma, like <forward>,<reverse>
            ''')

        # bams
        elif pathlib.PurePath.suffix(inputs[0][0]) in VALID_BAM_EXTS:
            
            if not all([
                pathlib.PurePath.suffix(x[0]) in VALID_BAM_EXTS 
                and pathlib.PurePath.suffix(x[1]) in VALID_INDEXED_BAM_EXTS 
                for x in inputs
            ]):
                raise ValueError('''
            Error with bam files. Input fastq files should end in ".bam" and ".bai". 
            Non-indexed and indexed bam files should be separated by a comma, like <bam>,<bai>
            ''')

        # otherwise throw an error
        else:
            raise ValueError('''
        Manifest file must be of one of the following forms: 
        1. Fastq inputs: 
            sample_1.fq,sample_2.fq

        2. Bam inputs:
            sample.bam,sample.bai
        ''')


    def _validate_reference(self):
        '''Ensure that the file path is not null and ends with .fa or .fasta
        '''
        
        ref_file = self.config_dict.get(INPUT_KEY, {}).get(REFERENCE_KEY, '')

        if ref_file == '' or pathlib.PurePath.suffix(ref_file) not in VALID_REF_EXTS:
            raise ValueError(f'''
        Reference file name is either blank or does not end in valid file extensions:
        {VALID_REF_EXTS}
        ''')

    def _validate_dirs(self):
        '''Make sure that the directory paths are not empty
        '''
        
        if self.config_dict.get(ARGS_KEY, {}).get(TEMP_DIR_KEY, '') is in {None, ''}:
            self.config_dict[ARGS_KEY][TEMP_DIR_KEY] = './temp/'

        if self.config_dict.get(ARGS_KEY, {}).get(LOG_DIR_KEY, '') is in {None, ''}:
            self.config_dict[ARGS_KEY][LOG_DIR_KEY] = './logs/'

        if self.config_dict.get(ARGS_KEY, {}).get(TEMP_DIR_KEY, '') is in {None, ''}:
            raise ValueError('''
        Invalid output directory
        ''')
        

    
def load_config(file: str):
    

    service = aws, gcp, local service type

