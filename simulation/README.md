# Simlulated SV calling Pipeline 
This pipeline, at a high level, runs through the following steps:
1. Simulates reads at a specified coverage from some genome
2. Aligns those reads
3. Calls structural variants 

The tools used in the pipeline itself are: 
1. [wgsim](https://github.com/lh3/wgsim): simulation
2. [bwa](https://github.com/lh3/bwa): indexing and alignments
3. [samtools](http://www.htslib.org/): indexing, sorting, extracting discordant reads and split reads
4. [lumpy](https://github.com/arq5x/lumpy-sv): structural variant calling
5. [bcftools](http://samtools.github.io/bcftools/bcftools.html): extracting breakpoints from vcfs

Additionally, two scripts are included for converting from breakpoints to bed files and expanding the sv calls by the specified amount. 

The output of the pipelin is a file called `<sample_name>_expanded_<expansion_size>_svs.bed`.

## Installation
In order to run this pipeline, you must have conda installed. Steps to install conda for your OS are found [here](https://conda.io/projects/conda/en/latest/user-guide/install/index.html). 

Once you have installed conda, you can then install snakemake. Instructions are found [here](https://snakemake.readthedocs.io/en/stable/getting_started/installation.html). If conda installed correctly, you should be able to install snakemake with the following command: 
```bash
$ conda install -n base -c conda-forge mamba
```

Finally clone the repo using
```bash
cd ~
git clone https://github.com/mchowdh200/animal_svs.git
```

## Running the pipeline
As of now, all configuration changes must be done in the `Snakefile`. The bare minimum that you must specifiy is the path to your `.fasta` or `.fa` genome reference file. The optional configurables are:
1. `OUTPUT_DIR`: where you would like all of the output of the pipeline to be stored. Default is the directory the snakefile is run from 
2. `wgsim_seed`: the seed to for simulation. Default uses the python random library to find a random seed
3. `coverage`: what depth you would like to simulate reads from the genome to. Default is 20
4. `base_error_rate`: the rate at which base errors will be simulated. Default is .001 to simulate Illumina sequencing error rate
5. `expansion_size`: the number of base pairs to extend SVs by at the very end of the pipeline. This value is added to the start and end of the SV. Default is 1000. 

There are more simulation parameters to tinker with, however we leave most of them at the `wgsim` defaults. 

To run the pipeline, make sure you have activated the snakemake environment with 
```bash
$ conda activate snakemake
```
then make sure you're in the correct directory
```bash
$ cd ~/animal_svs/simulation/
```
After changing the configurations in the Snakefile, run the following command: 
```bash
$ snakemake --use-conda --cores <desired_cores>
```

## Troubleshooting and issues
There are a few known issues you may run into:
1. If your coverage is set too high, you may run out of disk space. For example, running this pipeline on the human genome at 100x coverage resulting in R1 and R2 fastqs that were EACH 360 GB with a resulting bam file of about 200 GB. 
2. If your coverage is too low, you may get an error in the `extract_breakpoints` rule about an invalid file type. This is because your coverage was too low and no SVs were called. `bcftools` will throw an error because the `.vcf` will contain a header without any SVs

If you run into either of these issues, you can simply restart the pipeline with different parameters. 

__NOTE__: make sure if you set the seed manually, when restarting the pipeline you either: 
1. run it again with the `-R` parameter to force a full rerun or 
2. you remove the old files

Snakemake will see these old files, and without doing either of the above steps, snakemake will start at the rule with the last available output file

## Improvements
1. Move the python code at the top of the snakefile that gets the genome size to a rule (see the note above the python code)
2. Extract any configurables to a `config.yaml` and build a config loader
3. Use best-practices programming to clean up the snakefile a bit
4. Extract the overlapping rules of this snakefile and other snakefiles in this repo to a "core" snakefile (rules like `samtools faidx`, `bwa mem`, etc.)