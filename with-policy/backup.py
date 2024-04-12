# Databricks notebook source
# MAGIC %pip install jsonlines
# MAGIC %pip install tqdm

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

import jsonlines
import os
from tqdm import tqdm

if(os.path.isfile('/tmp/test.json')):
  os.remove("/tmp/test.json")


job_ids = get_job_ids()
l = len(job_ids)
with tqdm(total=l) as pbar:
    for job_id in job_ids:
        job_spec = get_job_spec(job_id)
        with jsonlines.open("/tmp/test.json", "a") as writer:   # for writing
            job_spec = get_job_spec(job_id)
            writer.write(job_spec)
        pbar.update(1)

dbutils.fs.cp('file:/tmp/test.json','/tmp/job.json',True)

# COMMAND ----------


