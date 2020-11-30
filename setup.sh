# conda setup -------------------------------------------------------------------------------------
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
bash ~/miniconda.sh -b -p $HOME/miniconda
eval "$($HOME/miniconda/bin/conda shell.bash hook)"
conda init
conda config --add channels bioconda

# snakemake installation --------------------------------------------------------------------------
conda install -c conda-forge mamba
mamba create -c conda-forge -c bioconda -n snakemake snakemake
