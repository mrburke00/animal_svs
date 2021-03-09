#!/bin/env bash

set exuo pipefail

## some common dependencies
sudo apt-get update
sudo apt-get install -y \
    build-essential curl git libcurl4-openssl-dev wget \
    software-properties-common automake libtool pkg-config libssl-dev \
    ncurses-dev awscli python-pip libbz2-dev liblzma-dev unzip

## mount storage
## TODO change to raid 0
export TMPDIR=/mnt/local/temp
sudo mkfs -t ext4 /dev/nvme0n1 

sudo mkdir /mnt/local
sudo mount /dev/nvme0n1 /mnt/local
sudo chown ubuntu /mnt/local

mkdir /mnt/local/data
mkdir /mnt/local/temp
echo 'export TMPDIR=/mnt/local/temp' >> ~/.profile

## tmux/neovim setup
echo 'source-file ~/.tmux.d/.tmux.conf' > ~/.tmux.conf
git clone https://github.com/mchowdh200/.tmux.d.git ~/.tmux.d

sudo add-apt-repository ppa:neovim-ppa/stable -y
sudo apt-get update -y
sudo apt-get install -y neovim
git clone https://github.com/mchowdh200/.vim.git ~/.vim
mkdir ~/.config
mkdir ~/.config/nvim
printf 'set runtimepath^=~/.vim runtimepath+=~/.vim/after\nlet &packpath=&runtimepath\nsource ~/.vim/vimrc' > ~/.config/nvim/init.vim
pip install jedi neovim
echo 'alias vim=nvim' >> ~/.profile
echo 'export EDITOR=nvim' >> ~/.profile

## setup path
mkdir /mnt/local/bin
echo 'PATH=$PATH:/mnt/local/bin' >> ~/.profile

## install gargs
wget https://github.com/brentp/gargs/releases/download/v0.3.9/gargs_linux -O /mnt/local/bin/gargs
chmod +x /mnt/local/bin/gargs

## conda setup
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
bash ~/miniconda.sh -b -p $HOME/miniconda
eval "$($HOME/miniconda/bin/conda shell.bash hook)"
conda init
conda config --add channels bioconda

## setup snakemake
mamba create -c bioconda -y -n snakemake snakemake boto3 boto
