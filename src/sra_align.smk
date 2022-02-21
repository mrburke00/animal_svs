import os
import yaml


with open("/scratch/Shares/layer/workspace/devin_sra/sv_step/sv_pipe/src/config.yaml", 'r') as stream:
    data_loaded = yaml.safe_load(stream)


ref_url = data_loaded['ref_url']
ref_dir = data_loaded['refdir']
out_dir = data_loaded['outdir']
home_dir = data_loaded['homedir']
organism_name = data_loaded['organism_name']
file_type = data_loaded['file_type']
file_index = data_loaded['file_index']


samples = ['TEST', 'TEST2']

rule all:
	input:
		f'{out_dir}/{{sample}}/{{sample}}.sorted.bam'
		#expand(out_dir + '/{sample}' + '/{sample}' + '.sorted.' + file_type, sample = samples)
#               expand(out_dir + '/{sample}/{sample}.sorted.' + file_type, sample = samples),
#               expand(out_dir + '/{sample}/{sample}.sorted.' + file_type + '.' + file_index, sample = samples)

rule test:
	output:
		f'{out_dir}/{{sample}}.{file_type}'
	shell:
		f"""
		echo {{wildcards}}
		echo {out_dir}/{{wildcards.sample}}.{file_type}
		"""
			
#rule fetch_sra:
#	output:
#		f'{out_dir}/{{sample}}_1.fastq',
#		f'{out_dir}/{{sample}}_2.fastq'	
#	shell:
#		f"""
#		echo {out_dir}/{{wildcards.sample}}_1.fastq
#		echo {out_dir}/{{wildcards.sample}}_2.fastq
#		"""

#rule align_fastq:
#	output:
#		f'{out_dir}/{{samples}}.{file_type}'
#	input:
#		f'{out_dir}/{{sample}}_1.fastq',
#		f'{out_dir}/{{sample}}_2.fastq' 
#	shell:
