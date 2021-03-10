# TODO make shell scripts and place them in the scripts directory to replace
# the strings in the shell directives?
"""
Multisample version of snakefile.  At the moment just testing with BAM/CRAM
as the starting point.  Will incorporate fastq alignment rule later.
"""
import os
# import config_utils
import boto3


################################################################################
## Setup
################################################################################
# configfile: "config.yaml"
# conf = config_utils.Config(config)
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

s3_ref_loc='layerlabcu/cow/ARS-UCD1.2_Btau5.0.1Y.fa'

################################################################################
## Rules
################################################################################
rule all:
    input:
        expand(f'{outdir}/{{sample}}/{{sample}}-smoove.genotyped.vcf.gz',
               sample=samples)


rule GetData:
    ## TODO use delegate functions based on remote/local
    ## to get outputs and relevant shell command
    resources:
        disk_mb = lambda wildcards: bam_size_bytes[wildcards.sample]//1000000
    output:
        # TODO make the temp() designation an option, just in case someone
        # wants to keep the bams/fastqs, locally after running the pipeline
        # * Could just use the --notemp flag on the snakemake command
        bam = temp(f'{outdir}/{{sample}}.bam'),
        index = temp(f'{outdir}/{{sample}}.{bam_index_ext}')
    log:
        f'{outdir}/log/{{sample}}.get_data.log'
    shell:
        # TODO just use boto3 or try the remote() wrapper
        # now that we are using the latest version of snakemake
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
        # TODO just use boto3 or try the remote() wrapper
        # now that we are using the latest version of snakemake
        f"""
        aws s3 cp s3://{s3_ref_loc} {{output.fasta}} 2> {{log}}
        aws s3 cp s3://{s3_ref_loc}.fai {{output.fai}} 2> {{log}}
        """


rule HighCovRegions:
    input:
        bam = f'{outdir}/{{sample}}.bam',
        bai = f'{outdir}/{{sample}}.{bam_index_ext}',
        fasta = f'{refdir}/ref.fa',
        fai = f'{refdir}/ref.fa.fai',
    params:
        prefix = f'{outdir}/{{sample}}/{{sample}}'
    output:
        quantized = temp(f'{outdir}/{{sample}}/{{sample}}.quantized.bed.gz'),
        high_cov= temp(f'{outdir}/{{sample}}/{{sample}}.high_cov.bed')
    conda:
        'envs/mosdepth.yaml'
    shell:
        # TODO make the high cov bin a config param
        """
        mosdepth --no-per-base \\
                 --fast-mode \\
                 --fasta {input.fasta}
                 --quantize 0:150: \\
                 {params.prefix} {input.bam}
        zgrep MOSDEPTH_Q1 {output.quantized} > {output.high_cov}"""
    

rule GapRegions:
    input:
        f'{refdir}/ref.fa'
    output:
        f'{outdir}/gap_regions.bed'
    conda:
        'envs/biopython.yaml'
    shell:
        'python scripts/gap_regions.py {input} > {output}'
    

rule ExcludeRegions:
    input:
        gap_bed = f'{outdir}/gap_regions.bed',
        high_cov= f'{outdir}/{{sample}}/{{sample}}.high_cov.bed'
    output:
        f'{outdir}/{{sample}}/{{sample}}.exclude.bed'
    conda:
        'envs/bedtools.yaml'
    shell:
        # TODO make max merge distance a config param
        """
        cat {input.gap_bed} {input.high_cov} |
            bedtools sort -i stdin |
            bedtools merge -d 10 -i stdin > {output}"""

rule SmooveCall:
    input:
        bam = f'{outdir}/{{sample}}.bam',
        bai = f'{outdir}/{{sample}}.{bam_index_ext}',
        fasta = f'{refdir}/ref.fa',
        fai = f'{refdir}/ref.fa.fai',
        exclude = f'{outdir}/{{sample}}/{{sample}}.exclude.bed'
    output:
        f'{outdir}/{{sample}}/{{sample}}-smoove.genotyped.vcf.gz'
    conda:
        'envs/smoove.yaml'
    shell:
        f"""
        smoove call --genotype \\
                    --duphold \\
                    --removepr \\
                    --fasta {{input.fasta}} \\
                    --exclude {{input.exclude}} \\
                    --name {{wildcards.sample}} \\
                    --outdir {outdir}/{{wildcards.sample}} \\
                    {{input.bam}}
        """

### TODO merge all the sites with smoove

### TODO Regenotype after merging?
# Would need to require redownloading data
