import os
# import config_utils
import boto3

################################################################################
## Setup
################################################################################
### TODO I'm repeating myself here with some of these rules
# rules can be imported or even inherited so we can look into that

### TODO test with hardcoded paths
refdir = '/mnt/local/data/ref'
outdir = '/mnt/local/data'

### TODO Test with hard coded buckets
s3_bam_bucket = 'layerlabcu/cow/bams/'
bucket_name, prefix = s3_bam_bucket.split('/', 1)
botoS3 = boto3.resource('s3')
my_bucket = botoS3.Bucket(bucket_name)
objs = my_bucket.objects.filter(Prefix=prefix, Delimiter='/')

### TODO add support for CRAM/CRAI                                                     
bam_list = [x.bucket_name + '/' + x.key for x in objs if x.key.endswith('.bam')]
bai_list = [x.bucket_name + '/' + x.key for x in objs if x.key.endswith('.bai')]
bam_index_ext = 'bam.bai' if bai_list[0].endswith('.bam.bai') else 'bai'

# get the size of the files ahead of time so that snakemake knows
# disk resource footprint prior to DAG creation
bam_size_bytes = {os.path.basename(x.key).rstrip('.bam'): x.size
                  for x in objs if x.key.endswith('.bam')}
samples = [x.lstrip(s3_bam_bucket).rstrip('.bam') for x in bam_list]

s3_ref_loc='layerlabcu/cow/ARS-UCD1.2_Btau5.0.1Y.prepend_chr.fa'

def bam_disk_usage(wildcards):
    return bam_size_bytes[wildcards.sample]//1000000

rule AllGenotype:
    input:
        f'{outdir}/sites.smoove.square.vcf.gz'

checkpoint GetData:
    resources:
        disk_mb = bam_disk_usage
    output:
        bam = temp(f'{outdir}/{{sample}}.bam'),
        index = temp(f'{outdir}/{{sample}}.{bam_index_ext}')
    log:
        f'{outdir}/log/{{sample}}.get_data.log'
    shell:
        f"""
        aws s3 cp s3://{s3_bam_bucket}{{wildcards.sample}}.bam {{output.bam}}  2> {{log}}
        aws s3 cp s3://{s3_bam_bucket}{{wildcards.sample}}.bai {{output.index}} 2>> {{log}}
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
    threads: 2
    input:
        bam = f'{outdir}/{{sample}}.bam',
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
                        --removepr \\
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
