# Databricks notebook source
# MAGIC %md 
# MAGIC This notebook is design to replace the job cluster conf key
# MAGIC
# MAGIC To use it, you just need to provide the policy ID, the name of the current conf key and the new conf key
# MAGIC
# MAGIC Please run with Debug->true or DRY_RUN->True first to make sure the change is as you expected. 

# COMMAND ----------

# MAGIC %pip install tqdm

# COMMAND ----------

dbutils.widgets.text("policy_id","required")
dbutils.widgets.text("conf","required")
dbutils.widgets.text("new_conf","required")
dbutils.widgets.dropdown("DEBUG","True", ["True", "False"])
dbutils.widgets.dropdown("DRY_RUN","True", ["True", "False"])


# COMMAND ----------

policy_id=dbutils.widgets.get("policy_id")
conf=dbutils.widgets.get("conf")
new_conf=dbutils.widgets.get("new_conf")
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
          args = [job_id, policy_id, conf, new_conf]
          futures.append(executor.submit(update_job, *args))
        for future in futures:
          result = future.result()
          pbar.update(1)

# COMMAND ----------



# COMMAND ----------


