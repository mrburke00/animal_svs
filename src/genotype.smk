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

#bucket_name, prefix = s3_bam_bucket.split('/', 1)
#botoS3 = boto3.resource('s3')
#my_bucket = botoS3.Bucket(bucket_name)
#objs = my_bucket.objects.filter(Prefix=prefix, Delimiter='/')
#
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

samples = ["ERR4056961", "SRR13786946", "SRR13786957", "SRR13786968", "SRR13786979", "SRR13786990",        \
        "ERR4336881",  "ERR875323",    "SRR13786947",  "SRR13786958",  "SRR13786969",  "SRR13786980", "SRR13786991",    \
        "ERR4336896",  "ERR875325",    "SRR13786948",  "SRR13786959",  "SRR13786970",  "SRR13786981",  "SRR13786992",   \
        "ERR4336897",  "ERR875326",    "SRR13786949",  "SRR13786960",  "SRR13786971",  "SRR13786982",  "SRR13786993",   \
        "ERR4336903",  "ERR875327",    "SRR13786950",  "SRR13786961",  "SRR13786972",  "SRR13786983",  "SRR13786994",   \
        "ERR4336912",  "SRR13786940",  "SRR13786951",  "SRR13786962",  "SRR13786973",  "SRR13786984",  "SRR13786995",   \
        "ERR4336924",  "SRR13786941",  "SRR13786952",  "SRR13786963",  "SRR13786974",  "SRR13786985",                   \
        "ERR4336927",  "SRR13786942",  "SRR13786953",  "SRR13786964",  "SRR13786975",  "SRR13786986",                   \
        "ERR875316",  "SRR13786943", "SRR13786954",  "SRR13786965",  "SRR13786976",  "SRR13786987",                     \
        "ERR875318",   "SRR13786944",  "SRR13786955",  "SRR13786966",  "SRR13786977",  "SRR13786988",                   \
        "ERR875320",   "SRR13786945",  "SRR13786956",  "SRR13786967",  "SRR13786978",  "SRR13786989"]



def bam_disk_usage(wildcards):
	return 16000
	#return bam_size_bytes[wildcards.sample]//1000000
################################################################################
## Rules
################################################################################
rule AllGenotype:
    input:
        f'{outdir}/sites.smoove.square.vcf.gz'

checkpoint GetData:
    ## TODO use delegate functions based on remote/local
    ## to get outputs and relevant shell command
    ## TODO add checkpoint to run after this rule
    ##      so that downstream disk_mb calculations update
    resources:
	disk_mb = bam_disk_usage
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
        #aws s3 cp s3://{s3_ref_loc} {{output.fasta}} 2> {{log}}
        #aws s3 cp s3://{s3_ref_loc}.fai {{output.fai}} 2> {{log}}
        """

rule SmooveGenotype:
    resources:
        disk_mb = bam_disk_usage
    priority: 1
    threads: 1
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
