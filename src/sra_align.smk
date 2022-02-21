import os
import yaml


with open("/scratch/Shares/layer/workspace/devin_sra/sv_step/sv_pipe/src/config.yaml", 'r') as stream:
    data_loaded = yaml.safe_load(stream)


ref_url = data_loaded['ref_url']
ref_dir = data_loaded['refdir']
out_dir = data_loaded['outdir']
home_dir = data_loaded['homedir']
organism_name = data_loaded['organism_name']


sra_numbers = []

rule all:
	input:
		



