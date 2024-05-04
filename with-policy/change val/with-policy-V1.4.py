# Databricks notebook source
# MAGIC %md 
# MAGIC This notebook is design to change the job cluster configuration value based on the conf key and policy 
# MAGIC
# MAGIC To use it, you just need to provide the policy ID, the name of the cluster conf and the desired new value .
# MAGIC
# MAGIC Please run with Debug->true or DRY_RUN->True first to make sure the change is as you expected. 

# COMMAND ----------

# MAGIC %pip install jsonlines
# MAGIC %pip install tqdm

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

dbutils.widgets.text("policy_id","required")
dbutils.widgets.text("attribute","required")
dbutils.widgets.text("value","required")
dbutils.widgets.dropdown("DEBUG","True", ["True", "False"])
dbutils.widgets.dropdown("DRY_RUN","True", ["True", "False"])


# COMMAND ----------

policy_id=dbutils.widgets.get("policy_id")
attribute=dbutils.widgets.get("attribute")
value=dbutils.widgets.get("value")
DEBUG=dbutils.widgets.get("DEBUG")
DRY_RUN=dbutils.widgets.get("DRY_RUN")

# COMMAND ----------

import requests
import json
import traceback
import time


#API_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
API_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
API_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
AUTH_HEADER = { 'Authorization': f"Bearer {API_TOKEN}" }

# DEBUG = False
# DRY_RUN = False

# COMMAND ----------

if DEBUG == "False":
  DEBUG = False
else:
  DEBUG = True

if DRY_RUN == "False":
  DRY_RUN = False
else :
  DRY_RUN = True

# COMMAND ----------

def update_job(job_id, policy_id, attribute, new_value):
    job_spec = get_job_spec(job_id)
    job_clusters = list(find_vals(job_spec, "job_clusters"))
    existing_cluster_id = list(find_vals(job_spec, "existing_cluster_id"))
    if len(existing_cluster_id)!=0 :
        print(job_id,"use exist cluster",existing_cluster_id)
    elif len(job_clusters)!=0 :
        print(job_id,"is in new formate")
        update_value(job_id, policy_id, attribute, new_value)
    else:
        print(job_id,"is in legacy formate")
        legacy_update_value(job_id, policy_id, attribute, new_value)

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm

num_cpu = 3
if __name__ == "__main__":  
    # Define a thread pool with 3 workers
    job_ids = get_job_ids()
    
    l = len(job_ids)
    with tqdm(total=l) as pbar:
      with ThreadPoolExecutor(max_workers=num_cpu) as executor:
        futures = []
        #start = time()
        for job_id in job_ids:
          args = [job_id, policy_id, attribute, value]
          futures.append(executor.submit(update_job, *args))
        for future in futures:
          result = future.result()
          pbar.update(1)


# COMMAND ----------

# from pyspark.sql.types import *
# spark.conf.set("spark.sql.legacy.json.allowEmptyString.enabled", True)

# schema1 = StructType([
#     StructField("created_time", LongType(), True),
#     StructField("creator_user_name", StringType(), True),
#     StructField("job_id", LongType(), True),
#     StructField("run_as_owner", BooleanType() , True),
#     StructField("run_as_user_name", StringType(), True),
#     StructField("settings", StructType(), True)
# ])
# df = spark.read.option("inferSchema","true").json("/tmp/job1.json",schema=schema1)
# display(df)

# COMMAND ----------


