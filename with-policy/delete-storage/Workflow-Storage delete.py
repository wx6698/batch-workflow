# Databricks notebook source
# MAGIC %pip install tqdm
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("policy_id","required")
dbutils.widgets.text("storage","required")
dbutils.widgets.dropdown("DEBUG","True", ["True", "False"])
dbutils.widgets.dropdown("DRY_RUN","True", ["True", "False"])


# COMMAND ----------

policy_id=dbutils.widgets.get("policy_id")
storage=dbutils.widgets.get("storage")
DEBUG=dbutils.widgets.get("DEBUG")
DRY_RUN=dbutils.widgets.get("DRY_RUN")

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

import requests
import json
import time
import traceback
from concurrent.futures import ThreadPoolExecutor


API_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
API_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
AUTH_HEADER = { 'Authorization': f"Bearer {API_TOKEN}" }


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

def update_job(job_id, policy_id, attribute):
    job_spec = get_job_spec(job_id)
    job_clusters = list(find_vals(job_spec, "job_clusters"))
    existing_cluster_id = list(find_vals(job_spec, "existing_cluster_id"))
    if len(existing_cluster_id)!=0 :
        print(job_id,"use exist cluster",existing_cluster_id)
    elif len(job_clusters)!=0 :
        print(job_id,"is in new formate")
        new_del_conf(job_id, policy_id, attribute)
    else:
        print(job_id,"is in legacy formate")
        legacy_del_conf(job_id, policy_id, attribute)

# COMMAND ----------


# storage_list = storage.split(",")
# print(storage_list)
# for storage in storage_list:         
#   exist = gen_storage_conf_v1(storage)
#   for old in exist:
#     update_job('1056447186701149', policy_id, old)         

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm


num_cpu = 16

if __name__ == "__main__":  
    # Define a thread pool with 3 workers
    job_ids = get_job_ids()
    storage_list = storage.split(",")
    l = len(job_ids)*len(storage_list)*5
    with tqdm(total=l) as pbar:
      with ThreadPoolExecutor(max_workers=num_cpu) as executor:
        futures = []
        #start = time()

        for storage in storage_list:
          exist = gen_storage_conf_v1(storage)
          for old in exist:
            for job_id in job_ids:
              args = [job_id, policy_id, old]
              futures.append(executor.submit(update_job, *args))
        for future in futures:
              result = future.result()
              pbar.update(1)
      #results = [future.result() for future in futures]
      #end = time()
      #print (end-start)

# COMMAND ----------


