# TODO make shell scripts and place them in the scripts directory to replace
# the strings in the shell directives
"""
Multisample version of snakefile.  At the moment just testing with BAM/CRAM
as the starting point.  Will incorporate fastq alignment rule later.
"""
import os
# import config_utils
import boto3
from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider


################################################################################
## Setup
################################################################################
# configfile: "config.yaml"
# conf = config_utils.Config(config)
refdir = "/mnt/local/test/ref"
outdir = "/mnt/local/test"

S3 = S3RemoteProvider()

# TODO Test with hard coded buckets -------------------------------------------
s3_bam_bucket = 'layerlabcu/cow/bams/'
bucket_name, prefix = s3_bam_bucket.split('/', 1)
botoS3 = boto3.resource('s3')
my_bucket = botoS3.Bucket(bucket_name)
objs = my_bucket.objects.filter(Prefix=prefix, Delimiter='/')
bam_list = [x.bucket_name + '/' + x.key for x in objs if x.key.endswith('.bam')]
samples = [x.lstrip(s3_bam_bucket).rstrip('.bam') for x in bam_list]

s3_ref_loc = 'layerlabcu/cow/ARS-UCD1.2_Btau5.0.1Y.fa'

################################################################################
## Rules
################################################################################
rule all:
    input:
        expand(outdir+"/{sample}/{sample}.txt", sample=samples),


rule get_data:
    output:
        # TODO make the temp() designation an option, just in case someone
        # wants to keep the bams/fastqs, locally after running the pipeline
        # * Could just use the --notemp flag on the snakemake command
        # TODO handle case of .bam.bai/.bai
        # TODO maybe don't need to have the bams in separate folders
        # Why? easier for folks who just have a bunch of bams in a local
        # directory, allowing this rule to be easily bypassed.
        bam = temp(outdir+"/{sample}.bam"),
        index = temp(outdir+"/{sample}.bai")
    log:
        outdir+"/log/{sample}.get_data.log"
    shell:
        """aws s3 cp s3://{s3_bam_bucket}{wildcards.sample}.bam {output.bam} --no-progress 2> {log}
        aws s3 cp s3://{s3_bam_bucket}{wildcards.sample}.bai {output.index} --no-progress 2>> {log}"""


rule test_get_data:
    input:
        bam = outdir+"/{sample}.bam",
        index = outdir+"/{sample}.bai"
    output:
        outdir+"/{sample}/{sample}.txt"
    conda:
        "envs/samtools.yaml"
    shell:
        "samtools view -H {input.bam} > {output}"


rule get_reference:
    output:
        fasta = temp(refdir+'/ref.fa')
        index = temp(refdir+'/ref.fa.fai')
    log:
        outdir+"/log/get_reference.log"
    shell:
        """aws s3 cp s3://{s3_ref_loc} {output.fasta} 2> {log}
        aws s3 cp s3://{s3_ref_loc}.fai {output.fasta} 2> {log}"""


rule high_cov_regions:
    input:
        outdir+"/{sample}.bam"
    params:
        prefix = outdir+"/{sample}/{sample}"
    output:
        quantized = temp(outdir+"/{sample}/{sample}.quantized.bed.gz")
        high_cov= temp(outdir+"/{sample}/{sample}.high_cov.bed")
    conda:
        "envs/mosdepth.yaml"
    shell:
        # TODO make the high cov bin a config param
        """mosdepth --no-per-base \
                    --fast-mode \
                    --fasta {get_reference.output.fasta}
                    --quantize 0:150: \
                    {params.prefix} {input}
           zgrep MOSDEPTH_Q1 {output.quantized} > {output.high_cov}"""
    

rule gap_regions:
    input:
        get_reference.output.fasta
    output:
        outdir+"/gap_regions.bed"
    conda:
        "envs/biopython.yaml"
    shell:
        "python scripts/gap_regions.py {input} > {output}"
    

rule exclude_regions:
    input:
        gap_bed = "{gap_regions.output}"
        high_cov = "{high_cov_regions.output.high_cov}"
    output:
        outdir+"/{sample}/{sample}.exclude.bed"
    conda:
        "envs/bedtools.yaml"
    shell:
        # TODO make max merge distance a config param
        "cat {input.gap_bed} {input.high_cov} |
             bedtools sort -i stdin |
             bedtools merge -d 10 -i stdin > {output}"

### TODO call/genotype SVs with smoove
    # use smoove call -g -d
rule smoove_call:
    input:
        bam = outdir+"/{sample}.bam"
        fasta = "{get_reference.output.fasta}"
        exclude = outdir+"/{sample}/{sample}.exclude.bed"
    params:
        output_dir = outdir+"/{sample}"
    output:
        outdir+"/{sample}/{sample}-smoove.genotyped.vcf.gz"
    conda:
        "envs/smoove.yaml"
    shell:
        """
        smoove call --genotype \
                    --duphold \
                    --removepr \
                    --fasta {input.fasta} \
                    --exclude {input.exclude} \
                    --name {wildcards.sample} \
                    --outdir {params.output_dir} \
                    {input.bam}
        """

### TODO merge all the sites with smoove

### TODO Regenotype after merging?
# Would need to require redownloading data
