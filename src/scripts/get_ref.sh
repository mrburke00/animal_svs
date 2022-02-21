#!/bin/bash
echo $1
echo $2

#HOW TO GET NAME FILE AND SAVE TO VARIABLE

cd $2

wget $1

gzip -d *.fna.gz

bwa index -p $3 *.fna

##chicken##
#wget https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/002/315/GCA_000002315.5_GRCg6a/GCA_000002315.5_GRCg6a_genomic.fna.gz

#mv GCA_000002315.5_GRCg6a_genomic.fna.gz data/refs/

#cd data/refs/

#gzip -d GCA_000002315.5_GRCg6a_genomic.fna.gz

#bwa index -p chicken GCA_000002315.5_GRCg6a_genomic.fna
