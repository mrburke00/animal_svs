#!/usr/bin/python
# expand a structural variant in a bed file

import argparse
import os

def expand_sv(start, stop, amount):
    return max(0, start - amount), stop + amount

def expand_svs(input_bed: str, output_bed: str, expansion_value: int):
    '''
    Expand the start, end positions by the expansion value on each side
    '''
    if not os.path.exists(input_bed):
        raise FileNotFoundError("File {} was not found".format(input_bed))

    with open(input_bed, 'r') as bp_file: 
        with open(output_bed, 'w') as op_file:
            for line in bp_file:
                chrom = line.split('\t')[0]
                start = int(line.split('\t')[1])
                stop = int(line.split('\t')[2])
                expanded_start, expanded_end = expand_sv(start, stop, expansion_value)

                op_file.write('{}\t{}\t{}\n'.format(chrom, expanded_start, expanded_end))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='expand a structural variant in a bed file')
    parser.add_argument('input_bed', type=str, help='.bed file to expand')
    parser.add_argument('output_bed', type=str, default=None, help='Name of the output file. Defaults to basename of input with _expanded appended')
    parser.add_argument('extension', type=int, default=100, help='Amount to add/subtract to the position. amount is subtracted from start AND added to end')

    args = parser.parse_args()

    input_bed = args.input_bed
    output_bed = args.output_bed

    if not output_bed:
        parent_dir = os.path.dirname(input_bed)
        f_name = os.path.basename(input_bed).split('.')[0] + '_expanded.bed'
        output_bed = os.path.join(parent_dir, f_name)

    expand_svs(input_bed, output_bed, args.extension)