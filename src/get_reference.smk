import os
import yaml
import io
parse = '/scratch/Shares/layer/workspace/devin_sra/sv_step/sv_pipe/src/scripts/'
sys.path.insert(1,parse)


with open("/scratch/Shares/layer/workspace/devin_sra/sv_step/sv_pipe/src/config.yaml", 'r') as stream:
    data_loaded = yaml.safe_load(stream)

ref_url = data_loaded['ref_url']
ref_dir = data_loaded['refdir']
out_dir = data_loaded['outdir']
home_dir = data_loaded['homedir']
organism_name = data_loaded['organism_name']      	

os.makedirs(home_dir, exist_ok = True)               
os.makedirs(ref_dir,  exist_ok = True)
os.makedirs(out_dir,  exist_ok = True)
print(expand(ref_dir + '/' + organism_name + '.{data_types}', data_types=['amb','ann','bwt','pac','sa']))
rule all:
	input:
		expand(ref_dir + '/' + organism_name + '.{data_types}', data_types=['amb','ann','bwt','pac','sa'])

rule get_reference:
	output:
		expand(ref_dir + '/' + organism_name + '.{data_types}', data_types=['amb','ann','bwt','pac','sa'])
	shell:
		'bash /scratch/Shares/layer/workspace/devin_sra/sv_step/sv_pipe/src/scripts/get_ref.sh {ref_url} {ref_dir} {organism_name}'


#pig.amb  pig.ann  pig.bwt  pig.pac  pig.sa
	
