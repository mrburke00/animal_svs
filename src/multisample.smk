"""
Multisample version of snakefile.  At the moment just testing with BAM/CRAM
as the starting point.  Will incorporate fastq alignment rule later.
"""
import os
# TODO add snakemake and boto install to setup script
from snakemake.remote.S3 import RemoteProvider as S3


### TODO hardcode s3 bucket for testing purposes.
# Later retreive from config.
### TODO don't require remote files
# s3_bucket = 's3://layerlabcu/cow/bams/'

### TODO get from config or from command line arg
# outdir = '/mnt/local/data'
# if not os.path.isdir(outdir): os.mkdir(outdir)

### TODO get "project_name" from config or command line arg
# project_name = "test"

configfile: "config.yaml"


################################################################################
## Rules
################################################################################
rule all:
    input:
        "{outdir}/merged/{project_name}-sites.vcf.gz"



### TODO Rule for getting bam paths from local or S3
    # conditional input to either use a local directory for bams or S3
    # controlled by config file params
        # s3: bool
        # s3_bucket: string -- bucket uri where the data is found
        # input_dir: string -- only used if s3 == False

rule get_data:
    """
    Conditional Rule:
      * gets the sample names from file names in data_dir/s3_bucket
      * if the data is in fastq format, then we expect the files to be
          {sample1}-1.fastq, {sample1}-2.fastq
          ...
          {sampleN}-1.fastq, {sampleN}-2.fastq
      * if the data is in BAM/CRAM format, then we expect the files to be
          {sample1}.{bam|cram}, {sample1}.{bai|crai}
          ...
          {sampleN}.{bam|cram}, {sampleN}.{bai|crai}
    """
    input:
        if
    


### TODO After Bam rules are done
# make conditional rule that gets fastqs or bam/cram
    # using glob_wildcards() check if the 

        



### TODO Rule(s) for getting high coverage regions with mosdepth
    # 1. Run mosdepth to get genome wide coverage
    # 2. Flag high cov regions (we can do this in mosdepth itself
    #    or after the fact using the coverage statistics)
    # 3. Merge close by regions (determined by some user defined metric)
    # 4. Final output is a bed file of high cov regions

### TODO get ref genome gap regions
    # input: reference fasta (s3 only for now)
    # uses extractGapRegions.py
    # output to bed file

### TODO merge high coverage regions with gap regions
    ## use 'bedtools merge -d $max_distance'

### TODO call/genotype SVs with smoove
    # use smoove call -g -d

### TODO merge all the sites with smoove
    # This is the Final step

