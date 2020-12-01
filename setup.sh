# conda setup -------------------------------------------------------------------------------------
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
bash ~/miniconda.sh -b -p $HOME/miniconda
eval "$($HOME/miniconda/bin/conda shell.bash hook)"
conda init
conda config --add channels bioconda

# snakemake installation --------------------------------------------------------------------------
conda install -c conda-forge mamba
mamba create -c conda-forge -c bioconda -n snakemake snakemake

# install the github repo and change into the src directory ---------------------------------------
git clone https://github.com/mchowdh200/animal_svs.git
cd animal_svs/src/

# activate snakemake ------------------------------------------------------------------------------
conda activate
conda activate snakemake