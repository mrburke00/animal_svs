import os
# import config_utils
#import numpy as np
#import pandas as pd
import yaml
import io
import sys
import boto3

with open("/scratch/Shares/layer/workspace/devin_sra/sv_step/sv_pipe/src/config.yaml", 'r') as stream:
    data_loaded = yaml.safe_load(stream)

refdir = data_loaded['refdir']
outdir = str(data_loaded['outdir'])
file_type = data_loaded['file_type']
file_index = data_loaded['file_index']
s3_bam_bucket = data_loaded['s3_bam_bucket']


bucket_name, prefix = s3_bam_bucket.split('/', 1)
botoS3 = boto3.resource('s3')
my_bucket = botoS3.Bucket(bucket_name)
objs = my_bucket.objects.filter(Prefix=prefix, Delimiter='/')

bam_list = [x.bucket_name + '/' + x.key for x in objs if x.key.endswith('.'+file_type)]
bai_list = [x.bucket_name + '/' + x.key for x in objs if x.key.endswith('.'+file_index)]
bam_size_bytes = {os.path.basename(x.key).rstrip('.'+file_type): x.size
	for x in objs if x.key.endswith('.'+file_type)}

bam_index_ext = file_type+'.'+file_index if bai_list[0].endswith('.'+file_type+'.'+file_index) else file_index

samples = [x.lstrip(s3_bam_bucket).rstrip('.'+file_type) for x in bam_list]

s3_ref_loc=data_loaded['s3_ref_loc']

for sample in samples:
	cmd = "aws s3 cp s3://"+s3_bam_bucket+sample+".bam "+outdir
	os.system(cmd)
	cmd2 = "aws s3 cp s3://"+s3_bam_bucket+sample+".bai "+outdir
	os.system(cmd2)
	break
