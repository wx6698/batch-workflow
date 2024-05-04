# Databricks notebook source
# MAGIC %md 
# MAGIC This notebook is design to add the spark.haoodp prefix to adls gen2 credentials configuration.
# MAGIC
# MAGIC To use it, you just need to provide the policy ID and the name of the Azure storage accounts seperate by ","
# MAGIC
# MAGIC Please run with Debug->true or DRY_RUN->True first to make sure the change is as you expected. 

# COMMAND ----------

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

if DEBUG == "False":
  DEBUG = False
else:
  DEBUG = True

if DRY_RUN == "False":
  DRY_RUN = False
else :
  DRY_RUN = True

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

def update_job(job_id, policy_id, attribute, new_attribute):
    job_spec = get_job_spec(job_id)
    job_clusters = list(find_vals(job_spec, "job_clusters"))
    existing_cluster_id = list(find_vals(job_spec, "existing_cluster_id"))
    if len(existing_cluster_id)!=0 :
        print(job_id,"use exist cluster",existing_cluster_id)
    elif len(job_clusters)!=0 :
        print(job_id,"is in new formate")
        update_conf(job_id, policy_id, attribute, new_attribute)
    else:
        print(job_id,"is in legacy formate")
        legacy_update_conf(job_id, policy_id, attribute, new_attribute)

# COMMAND ----------


# storage_list = storage.split(",")
# print(storage_list)
# for storage in storage_list:         
#   exist = gen_storage_conf_v1(storage)
#   for old in exist:
#     update_job('686738920871883', policy_id, old, "spark.hadoop."+old)              

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
              args = [job_id, policy_id, old, "spark.hadoop."+old]
              futures.append(executor.submit(update_job, *args))
        for future in futures:
              result = future.result()
              pbar.update(1)
      #results = [future.result() for future in futures]
      #end = time()
      #print (end-start)

# COMMAND ----------

# job_ids = get_job_ids()
# storage_list = storage.split(",")
# start = time()
# print(storage_list)
# for storage in storage_list:
#   print(storage)
#   exist = gen_storage_conf_v1(storage)
#   #print(exist)
#   for old in exist:
#     for job_id in job_ids:
#       #print(job_id, policy_id, old, "spark.hadoop."+old)
#       update_node(job_id, policy_id, old, "spark.hadoop."+old)
# end = time()

# print(end-start)

# COMMAND ----------


