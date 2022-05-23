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

with open("/scratch/Shares/layer/workspace/devin_sra/sv_step/config.yaml", 'r') as stream:
    data_loaded = yaml.safe_load(stream)

dir = data_loaded['dir']
ref_url = data_loaded['ref_url']
organism_name = data_loaded['organism_name']
ref_dir = dir + organism_name + '/ref'
out_dir = dir + organism_name + '/data'

with open(dir + "sra_runinfos/" + organism_name + '_sraRuns.txt') as f:
	samples = f.read().splitlines()

#if data_loaded['read_from_file']:
#
#	bam_list, bai_list, bam_size_bytes = readSamplesFile.get_samples(	      \
#							data_loaded['file_name'],     \
#							data_loaded['s3_bam_bucket'], \
#							data_loaded['file_type'],     \
#							data_loaded['file_index'])	
#
#else:
#
#	bucket_name, prefix = s3_bam_bucket.split('/', 1)
#	botoS3 = boto3.resource('s3')
#	my_bucket = botoS3.Bucket(bucket_name)
#	objs = my_bucket.objects.filter(Prefix=prefix, Delimiter='/')
#
#	bam_list = [x.bucket_name + '/' + x.key for x in objs if x.key.endswith('.'+file_type)]
#	bai_list = [x.bucket_name + '/' + x.key for x in objs if x.key.endswith('.'+file_index)]
#	bam_size_bytes = {os.path.basename(x.key).rstrip('.'+file_type): x.size
#                 for x in objs if x.key.endswith('.'+file_type)}
#
#bam_index_ext = file_type+'.'+file_index if bai_list[0].endswith('.'+file_type+'.'+file_index) else file_index
#
#samples = [x.lstrip(s3_bam_bucket).rstrip('.'+file_type) for x in bam_list]
#
#s3_ref_loc = data_loaded['s3_ref_loc']

def bam_disk_usage(wildcards):
	return 500000
	#return bam_size_bytes[wildcards.sample]//1000000
################################################################################
## Rules
################################################################################
rule AllGenotype:
    input:
        f'{outdir}/sites.smoove.square.vcf.gz'

checkpoint GetData:
#    resources:
#	disk_mb = bam_disk_usage
    output:
	# TODO make the temp() designation an option, just in case someone
        # wants to keep the bams/fastqs, locally after running the pipeline
        # * Could just use the --notemp flag on the snakemake command
        bam = f'{outdir}/{{sample}}/{{sample}}.sorted.{file_type}',
        index = f'{outdir}/{{sample}}/{{sample}}.sorted.{file_type}.{file_index}'
    log:
	f'{outdir}/log/{{sample}}.get_data.log'
    shell:
	# TODO just use boto3 or try the remote() wrapper
        # now that we are using the latest version of snakemake
        f"""
	echo {{wildcards.sample}}
        #aws s3 cp s3://{s3_bam_bucket}{{wildcards.sample}}.{file_type} {{output.bam}}  2> {{log}}
        #aws s3 cp s3://{s3_bam_bucket}{{wildcards.sample}}.{file_type}.{file_index} {{output.index}} 2>> {{log}}
        """
rule GetReference:
    output:
        fasta = temp(f'{refdir}/ref.fa'),
        fai = temp(f'{refdir}/ref.fa.fai')
    log:
        f'{outdir}/log/get_reference.log'
    shell:
        f"""
	echo "ref"
        """


rule SmooveGenotype:
    resources:
        disk_mb = bam_disk_usage
    priority: 1
    threads: 64
    input:
        #bam = f'{outdir}/{{sample}}.{file_type}',
        #bai = f'{outdir}/{{sample}}.{bam_index_ext}',
       	bam = f'{outdir}/{{sample}}/{{sample}}.sorted.{file_type}',
        bai = f'{outdir}/{{sample}}/{{sample}}.sorted.{file_type}.{file_index}',
	fasta = f'{refdir}/ref.fa',
        fai = f'{refdir}/ref.fa.fai',
        vcf = f'{outdir}/merged.sites.vcf.gz' 
    output:
        temp(f'{outdir}/{{sample}}/{{sample}}-smoove.genotyped.vcf.gz')
    conda:
        'envs/smoove.yaml'
    shell:
        f"""
        smoove genotype --processes 64 \\
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
