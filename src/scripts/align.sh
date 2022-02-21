#!/bin/bash


#sra_examples=("SRR4041813_2")
#sra_label_1=("943_LIB7850_LDI6492_GTGAAA_L002_R1_clean")
#sra_label_2=("943_LIB7850_LDI6492_GTGAAA_L002_R2_clean")

#aws s3api list-objects --bucket layerlabsra-pig --output text --query "Contents[].{Key: Key}" > tmp.txt

#python src/s3_help.py $1

#rm -r tmp.txt

#while read X Y Z; do sra_examples[++i]=$X; sra_label_1[i]=$Y; sra_label_2[i]=$Z; done < out.txt

#echo ${sra_examples[@]} , ${sra_label_1[@]} , ${sra_label_2[@]}

#rm -r out.txt
animal="mouse"
sra_examples=("$@")
for i in "${!sra_examples[@]}"; do
        prefetch "${sra_examples[i]}" --max-size 200G
        fastq-dump --split-files "${sra_examples[i]}"

        mv "${sra_examples[i]}"_1.fastq data/fastqs/
        mv "${sra_examples[i]}"_2.fastq data/fastqs/

        ## fastq -> bam ##
        bwa mem -M -t 16 -R "@RG\tID:1\tSM:""${sra_examples[i]}" \
        /scratch/Shares/layer/workspace/devin_sra/sra_step/data/refs/$animal \
        data/fastqs/"${sra_examples[i]}"_1.fastq data/fastqs/"${sra_examples[i]}"_2.fastq  \
        2> bwa_errors/bwa_"${sra_examples[i]}".err \
        > "${sra_examples[i]}".bam

        mv "${sra_examples[i]}".bam data/bams/

        ## sort bwa ##
        samtools sort data/bams/"${sra_examples[i]}".bam -o "${sra_examples[i]}".sorted.bam -@16 -m 8G

        mv "${sra_examples[i]}".sorted.bam data/bams/

        ## index bam ##
        samtools index data/bams/"${sra_examples[i]}".sorted.bam -@16

        mv "${sra_examples[i]}".sorted.bam.bai data/bams/

        ## upload to S3 ##
        aws s3 cp data/bams/"${sra_examples[i]}".sorted.bam s3://layerlabcu/sra/$animal/
        aws s3 cp data/bams/"${sra_examples[i]}".sorted.bam.bai s3://layerlabcu/sra/$animal/

        #rm -r data/bams/"${sra_examples[i]}".bam
        #rm -r data/bams/"${sra_examples[i]}".sorted.bam
        #rm -r data/bams/"${sra_examples[i]}".sorted.bam.bai
        rm -r data/fastqs/"${sra_examples[i]}"_1.fastq
        rm -r data/fastqs/"${sra_examples[i]}"_2.fastq
        #rm -r "${sra_examples[i]}"
done
