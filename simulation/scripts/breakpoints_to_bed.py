#!/usr/bin/python
# convert a "breakpoint" file of <chr>\t<pos>\t<cipos_lower,cipos_upper> to <chr>\t<start>\t<end>

import argparse
import os

def get_boundaries(csv_ci):
    lb = csv_ci.split(',')[0]
    ub = csv_ci.split(',')[1]

    if lb == '.':
        lb = 0

    if ub == '.':
        ub = 0

    return int(lb), int(ub)

def breakpoint_to_bed(breakpoint_file: str, output_file: str):
    '''
    Convert file
    '''
    if not os.path.exists(breakpoint_file):
        raise FileNotFoundError("File {} was not found".format(breakpoint_file))

    with open(breakpoint_file, 'r') as bp_file: 
        with open(output_file, 'w') as op_file:
            for line in bp_file:
                chrom = line.split('\t')[0]
                pos = int(line.split('\t')[1])
                lb, ub = get_boundaries(line.split('\t')[2])
                start = str(pos + lb)
                end = str(pos + ub)

                op_file.write('{}\t{}\t{}\n'.format(chrom, start, end))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='convert a "breakpoint" file of <chr>\\t<pos>\\t<cipos_lower,cipos_upper> to <chr>\\t<start>\\t<end>')
    parser.add_argument('breakpoint_file', type=str, help='The name of the file formatted as <chr>\\t<pos>\\t<cipos_lower,cipos_upper>')
    parser.add_argument('output', type=str, default=None, help='Name of the output file. Defaults to basename of input with .bed extension')

    args = parser.parse_args()

    breakpoint_file = args.breakpoint_file
    output = args.output

    if not output:
        parent_dir = os.path.dirname(breakpoint_file)
        f_name = os.path.basename(breakpoint_file).split('.')[0] + '.bed'
        output = os.path.join(parent_dir, f_name)

    breakpoint_to_bed(breakpoint_file, output)