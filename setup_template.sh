# set home to ubuntu since we dont know the user name
# and since our default machine is ubuntu 16.04
export HOME=/home/ubuntu

#--------- Install miniconda ---------#
echo "Installing miniconda...\n=============================="
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
bash ~/miniconda.sh -b -p $HOME/miniconda
eval "$($HOME/miniconda/bin/conda shell.bash hook)"
conda init
conda config --add channels bioconda
echo "Done"

#--------- Install snakemake ---------#
echo "\nInstalling snakemake...\n=============================="
conda install -c conda-forge mamba
mamba create -c conda-forge -c bioconda -n snakemake snakemake
conda activate snakemake
echo "Done"

#--------- Clone the repo ---------#
echo "\nCloning the repo...\n==========================="
cd $HOME
git clone https://github.com/mchowdh200/animal_svs.git
cd $HOME/animal_svs
echo "Done"

#--------- Install python dependencies ---------#
echo "\nInstalling python dependencies...\n==========================="
pip install -r $HOME/animal_svs/requirements.txt
echo "Done"

#--------- Activate snakemake ---------#
conda activate snakemake
