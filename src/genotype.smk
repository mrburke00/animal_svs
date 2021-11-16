import os
# import config_utils
import boto3
import yaml
import io
parse = '/scratch/Shares/layer/workspace/devin_sra/sv_step/sv_pipe/src/scripts/'
sys.path.insert(1,parse)
import readSamplesFile
################################################################################
## Setup
################################################################################

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

if data_loaded['read_from_file']:

	bam_list, bai_list, bam_size_bytes = readSamplesFile.get_samples(	      \
							data_loaded['file_name'],     \
							data_loaded['s3_bam_bucket'], \
							data_loaded['file_type'],     \
							data_loaded['file_index'])	

else:

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

s3_ref_loc = data_loaded['s3_ref_loc']

def bam_disk_usage(wildcards):
    return bam_size_bytes[wildcards.sample]//1000000
################################################################################
## Rules
################################################################################
rule AllGenotype:
    input:
        f'{outdir}/sites.smoove.square.vcf.gz'

checkpoint GetData:
    resources:
        disk_mb = bam_disk_usage
    output:
        bam = temp(f'{outdir}/{{sample}}.{file_type}'),
        index = temp(f'{outdir}/{{sample}}.{bam_index_ext}')
    log:
        f'{outdir}/log/{{sample}}.get_data.log'
    shell:
        f"""
        aws s3 cp s3://{s3_bam_bucket}{{wildcards.sample}}.{file_type} {{output.bam}}  2> {{log}}
        aws s3 cp s3://{s3_bam_bucket}{{wildcards.sample}}.{file_type}.{file_index} {{output.index}} 2>> {{log}}
        """

rule GetReference:
    output:
        fasta = temp(f'{refdir}/ref.fa'),
        fai = temp(f'{refdir}/ref.fa.fai')
    log:
        f'{outdir}/log/get_reference.log'
    shell:
        f"""
        aws s3 cp s3://{s3_ref_loc} {{output.fasta}} 2> {{log}}
        aws s3 cp s3://{s3_ref_loc}.fai {{output.fai}} 2> {{log}}
        """

rule SmooveGenotype:
    resources:
        disk_mb = bam_disk_usage
    priority: 1
    threads: 1
    input:
        bam = f'{outdir}/{{sample}}.{file_type}',
        bai = f'{outdir}/{{sample}}.{bam_index_ext}',
        fasta = f'{refdir}/ref.fa',
        fai = f'{refdir}/ref.fa.fai',
        vcf = f'{outdir}/merged.sites.vcf.gz' 
    output:
        temp(f'{outdir}/{{sample}}/{{sample}}-smoove.genotyped.vcf.gz')
    conda:
        'envs/smoove.yaml'
    shell:
        f"""
        smoove genotype --processes {{threads}} \\
                        --duphold \\
                        --fasta {{input.fasta}} \\
                        --name {{wildcards.sample}} \\
                        --outdir {outdir}/{{wildcards.sample}} \\
                        --vcf {{input.vcf}} \\
                        {{input.bam}}
        """

rule SmoovePaste:
    input:
        expand(f'{outdir}/{{sample}}/{{sample}}-smoove.genotyped.vcf.gz',
               sample=samples)
    output:
        f'{outdir}/sites.smoove.square.vcf.gz'
    conda:
        'envs/smoove.yaml'
    shell:
        f"""
        smoove paste --name sites \\
                     --outdir {outdir} \\
                     {{input}}
        """
