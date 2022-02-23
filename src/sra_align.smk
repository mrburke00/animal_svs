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
bwa_errors = data_loaded['bwa_err_log']
cores = data_loaded['cores']
samples = ['SRR18054682']

rule all:
	input:
#		f'{out_dir}/{{sample}}/{{sample}}.sorted.bam'
		expand(out_dir + '/{sampled}' + '/{sampled}' + '.sorted.' + file_type, sampled = samples),
		expand(out_dir + '/{sampled}' + '/{sampled}' + '.sorted.' + file_type + '.' + file_index, sampled = samples)
#               expand(out_dir + '/{sample}/{sample}.sorted.' + file_type, sample = samples),
#               expand(out_dir + '/{sample}/{sample}.sorted.' + file_type + '.' + file_index, sample = samples)

rule fetch_sra:
	output:
               f'{out_dir}/{{sample}}/{{sample}}_1.fastq',
               f'{out_dir}/{{sample}}/{{sample}}_2.fastq'
	shell:
		f"""
		cd {out_dir}/{{wildcards.sample}}

               	prefetch {{wildcards.sample}} --max-size 200G -O {out_dir}/{{wildcards.sample}}/
		fastq-dump --split-files -O {out_dir}/{{wildcards.sample}}/ {{wildcards.sample}}

		echo {out_dir}/{{wildcards.sample}}/{{wildcards.sample}}_1.fastq
               	echo {out_dir}/{{wildcards.sample}}/{{wildcards.sample}}_2.fastq

#               	touch {out_dir}/{{wildcards.sample}}/{{wildcards.sample}}_1.fastq
#               	touch {out_dir}/{{wildcards.sample}}/{{wildcards.sample}}_2.fastq		
		"""

rule align_fastq:
	output:
		f'{out_dir}/{{sample}}/{{sample}}.{file_type}'
	input:
		f1 = f'{out_dir}/{{sample}}/{{sample}}_1.fastq',
		f2 = f'{out_dir}/{{sample}}/{{sample}}_2.fastq' 
	shell:
		f"""
		echo {out_dir}/{{wildcards.sample}}/{{wildcards.sample}}.{file_type}

		bwa mem -M -t {cores} -R "@RG\tID:1\tSM:{{wildcards.sample}}" \\
		{ref_dir}/{organism_name} \\
		{{input.f1}} {{input.f2}}  \\
        	2> {bwa_errors}/bwa_{{wildcards.sample}}.err \\
        	> {{output}}

#			touch {out_dir}/{{wildcards.sample}}/{{wildcards.sample}}.{file_type}

		"""

rule sort:
	output:
		f'{out_dir}/{{sample}}/{{sample}}.sorted.{file_type}'
	input:
		f'{out_dir}/{{sample}}/{{sample}}.{file_type}'
	shell:
                f"""
                echo {out_dir}/{{wildcards.sample}}/{{wildcards.sample}}.sorted.{file_type}
		
		samtools sort {{input}} -o {{output}} -@{cores}

#			touch {out_dir}/{{wildcards.sample}}/{{wildcards.sample}}.sorted.{file_type}
               	"""

rule index:
	output:
		f'{out_dir}/{{sample}}/{{sample}}.sorted.{file_type}.{file_index}'
	input:
		f'{out_dir}/{{sample}}/{{sample}}.sorted.{file_type}'
	shell:
                f"""
                echo {out_dir}/{{wildcards.sample}}/{{wildcards.sample}}.sorted.{file_type}.{file_index} 
	
		samtools index {{input}} -@{cores}

#	        	touch {out_dir}/{{wildcards.sample}}/{{wildcards.sample}}.sorted.{file_type}.{file_index}


                rm {out_dir}/{{wildcards.sample}}/{{wildcards.sample}}_1.fastq
                rm {out_dir}/{{wildcards.sample}}/{{wildcards.sample}}_2.fastq
                rm {out_dir}/{{wildcards.sample}}/{{wildcards.sample}}.{file_type}
		rm -r {out_dir}/{{wildcards.sample}}/{{wildcards.sample}}
	        """
