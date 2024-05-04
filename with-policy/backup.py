# Databricks notebook source
# MAGIC %md 
# MAGIC This notebook is design to backup all workflows under your workspace
# MAGIC
# MAGIC The notebook will save the under the dbfs path you provided. The path is absolute dbfs path with filename such as dbfs:/tmp/workflow.json
# MAGIC
# MAGIC The json can be used later to create a workflow table

# COMMAND ----------

# MAGIC %pip install jsonlines
# MAGIC %pip install tqdm

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("path","dbfs:/tmp/job.json")

# COMMAND ----------

path=dbutils.widgets.get("path")

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

dbutils.fs.cp('file:/tmp/test.json',path,True)

# COMMAND ----------

# MAGIC %fs ls /tmp/

# COMMAND ----------


