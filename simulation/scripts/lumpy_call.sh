#!/bin/bash
# liberally taken from https://github.com/arq5x/lumpy-sv/blob/master/scripts/lumpy_smooth with permission from Ryan Layer

BAM_PREP()
{
    set -e
    BAM=$1
    ID=$2

    READ_LENGTH=`samtools view $BAM \
    | head -n 10000 \
    | gawk '
        BEGIN { MAX_LEN=0 } 
        { LEN=length($10); 
        if (LEN>MAX_LEN) MAX_LEN=LEN } 
        END { print MAX_LEN }'`

    samtools view $BAM \
        | tail -n+100000 \
        | python scripts/pairend_distro.py \
        -r $READ_LENGTH \
        -X 4 \
        -N 10000 \
        -o $OUTDIR/$ID.histo \
     > $OUTDIR/$ID.stats
}


GET_BAM_PARAMS () {
    set -e 

    BAM=$1
    ID=$2

    READ_LENGTH=`samtools view $BAM \
    | head -n 10000 \
    | gawk '
        BEGIN { MAX_LEN=0 } 
        { LEN=length($10); 
        if (LEN>MAX_LEN) MAX_LEN=LEN } 
        END { print MAX_LEN }'`
    
    MEAN=`cat $OUTDIR/$ID.stats | tail -n 1| cut -f1 | cut -d":" -f2`
    STD=`cat $OUTDIR/$ID.stats | tail -n 1| cut -f2 | cut -d":" -f2`
    PE_PARAM="-pe id:$ID,bam_file:$OUTDIR/$ID.disc.bam,histo_file:$OUTDIR/$ID.histo,mean:$MEAN,stdev:$STD,read_length:$READ_LENGTH,min_non_overlap:$READ_LENGTH,discordant_z:4,back_distance:20,weight:1,min_mapping_threshold:0 "

    GLOBAL_LUMPY+=$PE_PARAM

    SR_PARAM="-sr id:$ID,bam_file:$OUTDIR/$ID.split.bam,back_distance:10,weight:1,min_mapping_threshold:0 "

    GLOBAL_LUMPY+=$SR_PARAM
}
OUTDIR=$(cd $(dirname $1); pwd -P)/

GLOBAL_LUMPY="lumpy -mw 4 -t $(mktemp) -tt 0 "
BAM_PREP $1 $2 
GET_BAM_PARAMS $1 $2 

echo $GLOBAL_LUMPY
eval $GLOBAL_LUMPY