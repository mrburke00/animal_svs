"""
Multisample version of snakefile.  At the moment just testing with BAM/CRAM
as the starting point.  Will incorporate fastq alignment rule later.
"""
import os
# import config_utils
import boto3
import numpy as np
import pandas as pd
import yaml
import io

################################################################################
## Setup
################################################################################

### TODO add some way to check if the contigs
### in the FASTA ref match the BAM.

with open("config.yaml", 'r') as stream:
    data_loaded = yaml.safe_load(stream)
print(data_loaded


### TODO test with hardcoded paths
refdir = '/scratch/Shares/layer/workspace/devin_sra/sv_results/data/ref'
outdir = '/scratch/Shares/layer/workspace/devin_sra/sv_results/data'


### TODO Test with hard coded buckets
s3_bam_bucket = 'layerlabcu/sra/horseshoe_bat/'
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

s3_ref_loc='layerlabcu/ref/genomes/horshoe_bat/GCF_004115265.1_mRhiFer1_v1.p_genomic.fa'
# TODO fold this into remote/local specific OOP implementation
# eg if running on local data, we could just return 0 here
def bam_disk_usage(wildcards):
    return bam_size_bytes[wildcards.sample]//1000000
'''
################################################################################
## Rules
################################################################################
rule AllCall:
    input:
        f'{outdir}/merged.sites.vcf.gz' 

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
        bam = temp(f'{outdir}/{{sample}}.bam'),
        index = temp(f'{outdir}/{{sample}}.{bam_index_ext}')
    log:
        f'{outdir}/log/{{sample}}.get_data.log'
    shell:
        # TODO just use boto3 or try the remote() wrapper
        # now that we are using the latest version of snakemake
        f"""
        aws s3 cp s3://{s3_bam_bucket}{{wildcards.sample}}.bam {{output.bam}}  2> {{log}}
        aws s3 cp s3://{s3_bam_bucket}{{wildcards.sample}}.bam.bai {{output.index}} 2>> {{log}}
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

rule Mosdepth:
    resources:
        disk_mb = bam_disk_usage
    input:
        bam = f'{outdir}/{{sample}}.bam',
        bai = f'{outdir}/{{sample}}.{bam_index_ext}',
        fasta = f'{refdir}/ref.fa',
        fai = f'{refdir}/ref.fa.fai',
    params:
        prefix = f'{outdir}/{{sample}}/{{sample}}'
    output:
        temp(f'{outdir}/{{sample}}/{{sample}}.mosdepth.global.dist.txt'),
        temp(f'{outdir}/{{sample}}/{{sample}}.mosdepth.summary.txt'),
        temp(f'{outdir}/{{sample}}/{{sample}}.mosdepth.region.dist.txt'),
        temp(f'{outdir}/{{sample}}/{{sample}}.regions.bed.gz')
    conda:
        'envs/mosdepth.yaml'
    shell:
        """
        mosdepth --by 100 --fast-mode --fasta {input.fasta} \\
                 --no-per-base {params.prefix} {input.bam}
        """

rule GetHighCov:
    resources:
        disk_mb = bam_disk_usage
    input:
        mosdepth_bed = f'{outdir}/{{sample}}/{{sample}}.regions.bed.gz'
    output:
        high_cov_bed = f'{outdir}/{{sample}}/{{sample}}.high_cov.bed'
    run:
        mosdepth_bed = pd.read_csv(
            input.mosdepth_bed, compression='gzip', sep='\t',
            names=['chrom', 'start', 'end', 'depth'])

        # find regions with > 2 stddev above mean coverage
        mean_depth = mosdepth_bed.depth.mean()
        std_depth = mosdepth_bed.depth.std()
        high_cov_bed = mosdepth_bed.loc[
            mosdepth_bed.depth > (mean_depth + 2*std_depth)]
        high_cov_bed.to_csv(output.high_cov_bed, sep='\t',
                            header=False, index=False)

rule GapRegions:
    priority : 2
    threads:
        workflow.cores
    input:
        f'{refdir}/ref.fa'
    output:
        f'{outdir}/gap_regions.bed'
    conda:
        'envs/biopython.yaml'
    shell:
        'python /scratch/Shares/layer/workspace/devin_sra/sv_step/sv_pipe/src/scripts/gap_regions.py {input} {threads} > {output}'
    
rule ExcludeRegions:
    resources:
        disk_mb = bam_disk_usage
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
        cat {input.gap_bed} <(cut -f1-3 {input.high_cov}) |
            bedtools sort -i stdin |
            bedtools merge -d 10 -i stdin > {output}"""

rule SmooveCall:
    ## TODO list the rest of the outputs
    ## TODO mark them all as temp.
    priority: 1
    resources:
        disk_mb = bam_disk_usage
    input:
        bam = f'{outdir}/{{sample}}.bam',
        bai = f'{outdir}/{{sample}}.{bam_index_ext}',
        fasta = f'{refdir}/ref.fa',
        fai = f'{refdir}/ref.fa.fai',
        exclude = f'{outdir}/{{sample}}/{{sample}}.exclude.bed'
    output:
        temp(f'{outdir}/{{sample}}/{{sample}}-smoove.genotyped.vcf.gz')
    conda:
        'envs/smoove.yaml'
    shell:
        f"""
        smoove call --genotype \\
                    --duphold \\
                    --processes 1 \\
                    --fasta {{input.fasta}} \\
                    --exclude {{input.exclude}} \\
                    --name {{wildcards.sample}} \\
                    --outdir {outdir}/{{wildcards.sample}} \\
                    {{input.bam}}
        """

rule SmooveMerge:
    input:
        fasta = f'{refdir}/ref.fa',
        fai = f'{refdir}/ref.fa.fai',
        vcfs = expand(f'{outdir}/{{sample}}/{{sample}}-smoove.genotyped.vcf.gz',
                      sample=samples)
    output:
        f'{outdir}/merged.sites.vcf.gz'
    conda:
        'envs/smoove.yaml'
    shell:
        f"""
        smoove merge --name merged \\
                     --fasta {{input.fasta}} \\
                     --outdir {outdir} \\
                     {{input.vcfs}}
        """

# rule SmooveGenotype:
#     resources:
#         disk_mb = bam_disk_usage
#     priority: 1
#     threads: 12
#     input:
#         bam = f'{outdir}/{{sample}}.bam',
#         bai = f'{outdir}/{{sample}}.{bam_index_ext}',
#         fasta = f'{refdir}/ref.fa',
#         fai = f'{refdir}/ref.fa.fai',
#         vcf = f'{outdir}/merged.sites.vcf.gz' 
#     output:
#         temp(f'{outdir}/{{sample}}/{{sample}}-smoove.genotyped.vcf.gz')
#     conda:
#         'envs/smoove.yaml'
#     shell:
#         f"""
#         smoove genotype --processes {{threads}} \\
#                         --duphold \\
#                         --removepr \\
#                         --fasta {{input.fasta}} \\
#                         --name {{wildcards.sample}} \\
#                         --outdir {outdir}/{{wildcards.sample}} \\
#                         --vcf {{input.vcf}} \\
#                         {{input.bam}}
#         """
        
# rule SmoovePaste:
#     input:
#         expand(f'{outdir}/{{sample}}/{{sample}}-smoove.genotyped.vcf.gz',
#                sample=samples)
#     output:
#         f'{outdir}/sites.smoove.square.vcf.gz'
#     conda:
#         'envs/smoove.yaml'
#     shell:
#         f"""
#         smoove paste --name sites \\
#                      --outdir {outdir} \\
#                      {{input}}
#         """

'''
