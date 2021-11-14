"""
###
Script to parse s3 style urls into the correct input format for the pipeline rules
###
"""	
import boto3
import os

def get_samples(file_name, s3_bucket, file_type, file_index):

	ERR_serials = {}
	size_bytes = {}

	file = open(file_name, 'r')
	lines = file.readlines()
	for line in lines:
		words = line.split('/')
		if words[-2] not in ERR_serials.keys():
			HG_temps = words[-1].split('.')
			if HG_temps[0] not in ERR_serials.values():
				ERR_serials[words[-2]] = HG_temps[0]
	
	s3_bam_bucket = s3_bucket
	bucket_name, prefix = s3_bam_bucket.split('/', 1)	


	botoS3 = boto3.resource('s3')
	my_bucket = botoS3.Bucket(bucket_name)
	
	objs = my_bucket.objects.filter(Prefix=prefix)

	ERR = ERR_serials.keys()
	am_list = []
	ai_list = []
	for x in objs:
		matching = []
		matching = [s for s in ERR if s in x.key]
		if len(matching) == 1 and ERR_serials[matching[0]] in x.key:
			if x.key.endswith('.'+file_type):
				am_list.append(x.bucket_name + '/' + x.key)
				size_bytes[os.path.dirname(x.key).lstrip(s3_bam_bucket)+'/'+os.path.basename(x.key).rstrip('.'+file_type)] = x.size
		if x.key.endswith('.'+file_index):
			ai_list.append(x.bucket_name + '/' + x.key)
	#print(am_list)
	#print('------')
	#print(ai_list)
	return am_list, ai_list, size_bytes

#get_samples('/scratch/Shares/layer/workspace/devin_sra/sv_step/human_cram.txt', '1000genomes/1000G_2504_high_coverage/data/', 'cram', 'crai')
