# Animal SV calling pipeline
This pipeline, built with [snakemake](https://snakemake.readthedocs.io/en/stable/), is a structural variant calling pipeline. The original pipline, published in "The structural variation landscape in 492 Atlantic salmon genomes" (see references), was adapted to be more accessible to other research projects. Additionally, automatic cloud deployment (Active development) was added in order to simplify workloads. The inputs and outputs are:

### Inputs:
* Reference genome database, saved as an `.fa` file
* Forward and reverse sequence files, saved as `.fq` files

### Outpts:
* An index coverage html file from [goleft](https://github.com/brentp/goleft/tree/master/indexcov#indexcov)
* Genotyped `.vcf` file from [smoove](https://github.com/brentp/smoove)

This pipeline uses the following tools: 
* [bwa](http://bio-bwa.sourceforge.net/) 
    * reference genome indexing
    * local alignments
* [samtools](http://www.htslib.org/)
    * reference genome indexing
    * sorting alignments
    * indexing bam files
*  [goleft](https://github.com/brentp/goleft/tree/master/indexcov#indexcov)
    * index coverage visualizations
* [smoove](https://github.com/brentp/smoove)
    * variant calling of bam
    * genotyping on variant calls
* custom script for extracting gap regions

---
## Installation
To install the pipeline, simply run 
```bash
$> git clone https://github.com/mchowdh200/animal_svs.git
```

Then, in your favorite python development environment, run 
```bash
$> cd animal_svs
$animal_svs> pip install -r requirements.txt
```
---
## Running 
As of now, there are two available workflows: 
1. Local
2. GCP VM instance deployment

### Local
To run the pipeline locally, more dependencies are needed (if not installed already). To install these, run the following in your terminal:
```bash
$animal_svs> chmod u+x build.sh
$animal_svs> ./build.sh
```
This will install [minconda](https://docs.conda.io/en/latest/miniconda.html) and [snakemake](https://snakemake.readthedocs.io/en/stable/) as well as adding the bioconda channel. 

After this installation is complete, edit the `config.yaml` file found in the `src` directory. Please provide full paths, such as `/Users/<user_name>/Documents/path/to/files/` for all input files and output directories. Make sure that the value `type` under `run`, `deployment` is set to 'local'. 

An example config file for local deployment looks like:

```yaml
############### Parameters associated with the data ###############
input:

  # as of right now, we only support a single experiment
  # with a forward and reverse component
  samples:
    forward: '/home/<user_name>/animal_svs/data/samples/forward.fq'
    reverse: '/home/<user_name>/animal_svs/data/samples/reverse.fq'

  # reference genome database
  reference: '/home/<user_name>/animal_svs/data/reference.fa'

  # used to name output files. Not needed. If left blank,
  # defaults to the filename of the forward sequence
  sample_name: 'local-run'

############### Parameters associated with the run ###############
run:

  # temporary file store. Files will be deleted
  temp_dir: '/home/<user_name>/animal_svs/temp/'

  # output any issues/progress to this location
  logs_dir: '/home/<user_name>/animal_svs/logs/'

  # directory to save final output files
  output_dir: '/home/<user_name>/animal_svs/output/'

  # for deployment either locally or to the cloud
  deployment:

    # supported types: 'cloud', 'local'
    type: 'local'

    # number of cores to use. If left at 0, max cores are used
    cores: 0

    # NOTE: the rest of the yaml does not matter for local deployment
    # values below can have any value or you can remove them

```

Finally, to run the pipeline, do: 
```bash
$animal_svs> conda activate snakemake
(snakemake) $animal_svs> python run_pipeline.py
```
And the pipeline should start to run

### GCP Cloud Deployment
Before you can run this pipeline please make sure to have done the following:
1. Make sure you have `python` (v3.5+) installed ([installation instructions](https://www.python.org/downloads/))
2. Create a [Google Cloud Account](https://cloud.google.com/)
3. Setup a [project in Google Cloud](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
4. Make sure you have `gcloud` as a command on your machine and have the credentials to run `gcloud` commands
    * Installation instructions for `glcoud` can be found [here](https://cloud.google.com/sdk/gcloud)
    * This [QwickLabs](https://www.qwiklabs.com/focuses/2794?parent=catalog) tutorial may be helpful for first time users
    * Documentation for `gloud auth` commands can be found [here](https://cloud.google.com/sdk/gcloud/reference/auth/login)
5. Created a [Google Cloud Bucket](https://cloud.google.com/storage/docs/creating-buckets) with your data in it

Once you have done the steps above, you will want to edit the config file found in `src/config.yaml` An example config file for cloud deployment looks like: 

```yaml
############### Parameters associated with the data ###############
input:

  # as of right now, we only support a single experiment
  # with a forward and reverse component
  samples:
    forward: 'forward.fq'
    reverse: 'reverse.fq'

  # reference genome database
  reference: 'reference.fa'

  # used to name output files. Not needed. If left blank,
  # defaults to the filename of the forward sequence
  sample_name: 'cloud-deployment'

############### Parameters associated with the run ###############
run:

  # temporary file store. Files will be deleted
  temp_dir: '../temp'

  # output any issues/progress to this location
  logs_dir: '../logs'

  # directory to save final output files
  output_dir: 'output'

  # for deployment either locally or to the cloud
  deployment:

    # supported types: 'cloud', 'local'
    type: 'cloud'

    # number of cores to use. If left at 0, max cores are used
    cores: 0

    # Service is ONLY used if deployment is set to 'cloud'
    # current supported services are: 'gcp'
    service: 'gcp'

    # if deployment is cloud, we assume that files are stored in the cloud
    # therefore we need the bucket name. As of right now, we only support
    # storage that is the same as the deployment. So if deployment is 'gcp',
    # data must be stored in a gcp bucket. do NOT add the gs:// or s3:// prefix
    bucket_name: '<your-bucke-name>'

    # the project name. Right now, only supported for GCP project
    project_name: '<your-project-name>'

    # instance machine type
    # documentation for Google Cloud Compute Enginer machine types can be seen here
    # https://cloud.google.com/compute/docs/machine-types
    # NOTE: for extra large files, its advised to get at least 16 GB ram if not more
    # and at least the size of all files for the hard drive.
    gcp_instance:
        machine_type: 'e2-standard-4'
        disk_space: '40' # in GB
        ram_size: '16' # in GB

        # documentation for region and zone found here
        # https://cloud.google.com/compute/docs/regions-zones
        region: 'us-central1'
        zone: 'us-central1-a'
```

Finally, to deploy the pipeline, do
```bash
$animal_svs> python run_pipeline.py
```
---
## Current Limitations
As of now, the pipeline relies on the environment/operating system it is run on. Due to this, some tools are known to not work on certain operating systems (local runs) these are: 
* smoove
    * The smoove dependency pulled by snakemake does not have a build for either macOS nor Windows, so neither of these OSes will run the pipeline locally

---
## References: 
bwa: 

Li H. and Durbin R. (2009) Fast and accurate short read alignment with Burrows-Wheeler Transform. Bioinformatics, 25:1754-60. [PMID: 19451168]

Original paper: 

Bertolotti, Alicia C., et al. "The structural variation landscape in 492 Atlantic salmon genomes." bioRxiv (2020)

