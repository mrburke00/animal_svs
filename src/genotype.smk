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
samples = ["ERR4349374",  "ERR4350106",  "ERR4351078",  "ERR4351380", "ERR4351464",  "ERR4351491","SRR5827418",  "SRR9671970",                            \
"ERR4349451",  "ERR4350107",  "ERR4351093",  "ERR4351386",  "ERR4351465", "ERR4368125", "SRR13091899",  "SRR5827419",  "SRR9671971",  \
"ERR4335881",   "ERR4350081",  "ERR4351466",  "ERR4351496",  "ERR4351516",  "ERR571464",    "SRR5827420",  "SRR9671994",   \
"ERR4337735", "ERR4351295",  "ERR4351410",  "ERR4351497",  "ERR4351517",  "SRR12532408",  "SRR13537342",  "SRR5827421",  "SRR9671997", \
"ERR4339391",   "ERR4350116",  "ERR4351323",  "ERR4351415",  "ERR4351471",  "ERR4351498",  "ERR4351518",  "SRR10303810",  "SRR13537343", "SRR9672004",   \
"ERR4350087",  "ERR4351332",  "ERR4351472",  "ERR4351502",  "ERR4351520",  "SRR10303811", "SRR13537344",  "SRR9672015",  \
"ERR4350088",  "ERR4350148",  "ERR4351344",  "ERR4351454",  "ERR4351474",  "ERR4351506",  "ERR4351521",  "SRR10859027",   "SRR3041121",   "SRR8480636",  "SRR9672020",  \
"ERR4350091",  "ERR4350697",  "ERR4351346",  "ERR4351459",  "ERR4351475",  "ERR4351508",  "ERR4351523",  "SRR3041425",   "SRR8902348",  "SRR9672024",    \
"ERR4349095",   "ERR4351037",  "ERR4351359",  "ERR4351460",  "ERR4351476",  "ERR4351509", "SRR3041436",   "SRR9671907",  "SRR9672050", \
"ERR4349110",   "ERR4350104",  "ERR4351362",  "ERR4351461",  "ERR4351485",  "ERR4351511", "SRR3041443",   "SRR9671956",                                        \
"ERR4349369",   "ERR4350105",  "ERR4351044",  "ERR4351378",  "ERR4351462",  "ERR4351488",  "ERR4351512", "SRR12681028",  "SRR5827417",  "SRR9671961"]
#horse
#samples = ['ERR1034513', 'ERR657751', 'SRR10038924', 'SRR12719762', 'SRR4054277', 'SRR515214', 'SRR6474871', 'SRR6474892', 'SRR6474904', 'SRR9912866', 'SRR9912879', 'ERR1034517', \
#'ERR657758', 'SRR10038925', 'SRR12719766', 'SRR4054279', 'SRR515217', 'SRR6474872', 'SRR6474893', 'SRR6474905', 'SRR6650672', 'SRR9912867', 'SRR9912880', 'ERR3382886', 'ERR657889',   \
#'SRR1046147', 'SRR12719776', 'SRR505867', 'SRR516118', 'SRR6474873', 'SRR6474894', 'SRR6508245', 'SRR6650673', 'SRR9912868', 'SRR9912881', 'ERR3382887', 'ERR953404', 'SRR12719743',   \
#'SRR1564421', 'SRR5131680', 'SRR5469030', 'SRR6474874', 'SRR6474895', 'SRR6650675', 'SRR9912869', 'ERR3382891', 'ERR953412', 'SRR12719745', 'SRR1564423', 'SRR5131681',  \
#'SRR5469031', 'SRR6474875', 'SRR6474896', 'SRR6607268', 'SRR9912870', 'ERR3929504', 'ERR953413', 'SRR12719746', 'SRR2102500', 'SRR5131682', 'SRR5469032', 'SRR6474876',  \
#'SRR6474897', 'SRR6619605', 'SRR6650679', 'SRR9912871', 'ERR3929505', 'ERR978601', 'SRR12719747', 'SRR2102896', 'SRR5131683', 'SRR5591523', 'SRR6474877', 'SRR6474898', 'SRR6650662',  \
#'SRR6650680', 'SRR9912872', 'ERR4349451', 'ERR978602', 'SRR12719748', 'SRR2103372', 'SRR515203', 'SRR5591591', 'SRR6474878', 'SRR6474899', 'SRR6650663', 'SRR6650681', 'SRR9912874',   \
#'ERR4350083', 'ERR982794', 'SRR12719749', 'SRR4054238', 'SRR515204', 'SRR5591598', 'SRR6474888', 'SRR6474900', 'SRR6650665', 'SRR8074196', 'SRR9912875', 'ERR4351415', 'SRR10038920',  \
#'SRR12719750', 'SRR4054239', 'SRR515206', 'SRR6374293', 'SRR6474889', 'SRR6474901', 'SRR6650666', 'SRR8074197', 'SRR9912876', 'ERR5059415', 'SRR10038921', 'SRR12719753', 'SRR4054241',\
#'SRR515208', 'SRR6474869', 'SRR6474890', 'SRR6474902', 'SRR871534', 'SRR9912877', 'ERR657749', 'SRR10038923', 'SRR12719756', 'SRR4054242', 'SRR515213', 'SRR6474870',    \
#'SRR6474891', 'SRR6474903', 'SRR6650669', 'SRR9912865', 'SRR9912878']

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
