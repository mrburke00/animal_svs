#!bin/python
import gzip 
import io 
import sys 
import os

# This file will generate a bedfile of the masked regions a fasta file.
# get the file name from the snakemake input
reference_file = snakemake.input[0]

# Check file type
if reference_file.endswith(".fa.gz"):
    input_fasta = io.TextIOWrapper(io.BufferedReader(gzip.open(reference_file))) 

elif reference_file.endswith(".fa") or reference_file.endswith(".txt"):
    input_fasta = open(reference_file,'r') 

else:
    raise Exception("Unsupported File Type") 

n, state = 0, 0 # line, character, state (0=Out of gap; 1=In Gap) 
chrom, start, end = None, None, None

# open up the output file to write to
output_file = snakemake.output[0]
o = open(output_file, 'w')

with input_fasta as f: 
    for line in f:
        line = line.replace("\n","") 
        if line.startswith(">"):
        # Print end range 
            if state == 1:
                o.write('\t'.join([chrom ,str(start), str(n)]))
                start, end, state = 0, 0, 0
            n = 0 # Reset character
            chrom = line.split(" ")[0].replace(">","")

        else:
            for char in line:
                if state == 0 and char == "N":
                    state = 1   
                    start = n
                elif state == 1 and char != "N":
                    state = 0
                    end = n
                    o.write('\t'.join([chrom ,str(start), str(end)]))
                else: 
                    pass

            n += 1 # First base is 0 in bed format.

# Print mask close if on the last chromosome. 
if state == 1:
    o.write('\t'.join([chrom ,str(start), str(n)]))
    start, end, state = 0, 0, 0