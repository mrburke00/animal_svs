import yaml
import selectors
import subprocess
import logger

import multiprocessing as mp

from google.cloud import pubsub_v1

# message sent to logs when pipeline is finished
pipeline_complete_msg = 'Pipeline run complete'

# load config
config = yaml.safe_load(open('config.yaml'))

# start the logger
logger = logger.Logger()

# get the number of cores. If 0, use max cores
cores = mp.cpu_count() if int(config['run']['deployment']['cores']) == 0 else int(config['run']['deployment']['cores'])

#---------------- Start Snakemake and read from stdout and stderr ----------------------#
p = subprocess.Popen(
    ['snakemake', '--cores', f'{cores}', '--use-conda', '-F'], stdout=subprocess.PIPE, stderr=subprocess.PIPE
)

sel = selectors.DefaultSelector()
sel.register(p.stdout, selectors.EVENT_READ)
sel.register(p.stderr, selectors.EVENT_READ)

while True:

    is_done = False

    # check to see if the process is done
    # p.poll() is None evaulating to true means its still running
    if not p.poll() is None:
        is_done = True

    for key, _ in sel.select():
        data = key.fileobj.read1().decode()
        if not data:
            is_done = True

        msg = ''
        if key.fileobj is p.stdout:
            msg = str(data)
        else:
            msg = str(data)

        # log the output
        logger.log(msg)

    if is_done:
        logger.log(pipeline_complete_msg)
        break 
