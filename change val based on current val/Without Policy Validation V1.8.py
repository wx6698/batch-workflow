# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

dbutils.widgets.text("attribute","required")
dbutils.widgets.text("new_value","required")
dbutils.widgets.text("old_value","required")
dbutils.widgets.dropdown("DEBUG","True", ["True", "False"])
dbutils.widgets.dropdown("DRY_RUN","True", ["True", "False"])


# COMMAND ----------

attribute=dbutils.widgets.get("attribute")
new_value=dbutils.widgets.get("new_value")
old_value=dbutils.widgets.get("old_value")
DEBUG=dbutils.widgets.get("DEBUG")
DRY_RUN=dbutils.widgets.get("DRY_RUN")

# COMMAND ----------

import requests
import json
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

def get_job_ids():
    session = requests.Session()
    api_url = f"{API_URL}/api/2.1/jobs/list"
    api_params = { "limit": 20, "offset": 0 }
    
    try:
        response = session.get(api_url, headers=AUTH_HEADER, params=api_params)
        #response_data = response.json()["jobs"]
        job_ids = []
        #response.json()["jobs"]
        for job_id in response.json()["jobs"]:
            job_ids.append(job_id["job_id"])
        response_length = len(response.json()["jobs"])
        while response.json()["has_more"]:
            next_page_token = response.json()["next_page_token"]
            api_params = { "page_token":{next_page_token}}
            response = session.get(api_url, headers=AUTH_HEADER, params=api_params)
            for job_id in response.json()["jobs"]:
                #print(job_id["job_id"])
                job_ids.append(job_id["job_id"])
            #response_data.append(response.json()["jobs"])
            response_length += len(response.json()["jobs"])
    except requests.exceptions.RequestException:
        pass
    return job_ids


def get_job_spec(job_id):
    api_url = f"{API_URL}/api/2.1/jobs/get?job_id={job_id}"
    response = requests.get(api_url, headers=AUTH_HEADER)
    return response.json()


#def get_job(job_spec,policy_id):
    #print(job_spec)
    # for item in data['settings']['job_clusters']['new_cluster']:
    #   try:    
    #     for row in item['policy_id']:
    #       print (row)
    #   except KeyError as Ex:
    #     print ("{} not found in {}".format(Ex,item))


def find_vals(job_spec, conf_key):
    if isinstance(job_spec, list):
        for i in job_spec:
            for x in find_vals(i, conf_key):
                yield x
    elif isinstance(job_spec, dict):
        if conf_key in job_spec:
            yield job_spec[conf_key]
        for j in job_spec.values():
            for x in find_vals(j, conf_key):
                yield x

def change_vals(job_spec, conf_key, new_val, old_val):
    vals = list(find_vals(job_spec, conf_key))
    if vals: 
        job_spec_str = json.dumps(job_spec)
        job_spec_str = job_spec_str.replace(str(old_val), str(new_val))
        return json.loads(job_spec_str)
    else:
        if DEBUG:
            print('DEBUG: No change in', conf_key)
        return job_spec

def update_job_spec(job_id, new_job_spec):
    api_url = f"{API_URL}/api/2.1/jobs/update"
    new_settings = new_job_spec['settings']    
    response = requests.post(api_url, headers=AUTH_HEADER, json={ 'job_id': job_id, 'new_settings': new_settings })
    return response

def recursive_compare(d1, d2, level='root'):
    """https://stackoverflow.com/a/53818532"""
    if isinstance(d1, dict) and isinstance(d2, dict):
        if d1.keys() != d2.keys():
            s1 = set(d1.keys())
            s2 = set(d2.keys())
            print('{:<20} + {} - {}'.format(level, s1-s2, s2-s1))
            common_keys = s1 & s2
        else:
            common_keys = set(d1.keys())
        for k in common_keys:
            recursive_compare(d1[k], d2[k], level='{}.{}'.format(level, k))
    elif isinstance(d1, list) and isinstance(d2, list):
        if len(d1) != len(d2):
            print('{:<20} len1={}; len2={}'.format(level, len(d1), len(d2)))
        common_len = min(len(d1), len(d2))
        for i in range(common_len):
            recursive_compare(d1[i], d2[i], level='{}[{}]'.format(level, i))
    else:
        if d1 != d2:
            print('{:<20} {} != {}'.format(level, d1, d2))

def update_jobs(job_id, attribute, new_value,old_val):
    jobs_spec = []
    new_jobs_spec = []
    job_spec = get_job_spec(job_id)
    jobs_spec.append(job_spec)
    vals = list(find_vals(job_spec, attribute))
    if vals:
        if old_value in vals[0]:          
            new_job_spec = change_vals(job_spec, attribute, new_value,old_val)
            new_jobs_spec.append(new_job_spec)
            if DEBUG:
                print('DEBUG: Job ID: ', job_id)
                # print(new_job_spec)
                recursive_compare(job_spec, new_job_spec)
            if DRY_RUN:
                print('INFO: [Dry-run] Changes to job', job_id)
                recursive_compare(job_spec, new_job_spec)
            else:
                updated = update_job_spec(job_id, new_job_spec)
                if updated.status_code != 200:
                    print('ERROR: Updating job', job_id, updated.text())
                else:
                    print('INFO: Updated job', job_id)
        else:
            if DEBUG:
                print('DEBUG: Job ID: ', job_id,"current conf value does not match conf value given",old_val)
    else:
        if DEBUG:
                print('DEBUG: Job ID: ', job_id,"do not have the conf",attribute)
    # except:
    #     print("job id:",job_id,"failed to process")
    #     #print(job_spec)

# COMMAND ----------

# job_spec = get_job_spec('251897619430212')
# vals = list(find_vals(job_spec, attribute))
# if old_value in vals[0]:
#     print("find",vals)
# else:
#     print(vals)

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import functions as F
from time import sleep, time
from tqdm import tqdm


def backup_job_specs(schema, table, specs):
    current_time = F.current_timestamp()
    df = spark.createDataFrame(specs).withColumn("insert_time", current_time)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    df.write.format("delta").mode("append").saveAsTable(f"{schema}.{table}")


def run_backup(job_ids, num_cpu, schema, table):
    with tqdm(total=len(job_ids)) as pbar:
        with ThreadPoolExecutor(max_workers=num_cpu) as executor:
            futures = []
            result = []
            for job_id in job_ids:
                args = [job_id]
                futures.append(executor.submit(get_job_spec, *args))
            for future in futures:
                result.append(future.result())
                pbar.update(1)
        # Backup job specs in Delta table
        backup_job_specs(schema, table, result)

def run_update(job_ids, num_cpu):
    with tqdm(total=len(job_ids)) as pbar:
        with ThreadPoolExecutor(max_workers=num_cpu) as executor:
            futures = []
            for job_id in job_ids:
                args = [job_id, attribute, new_value, old_value]
                futures.append(executor.submit(update_jobs, *args))
            for future in futures:
                future.result()
                pbar.update(1)

# Example usage
if __name__ == "__main__":
    job_ids = get_job_ids()
    test_jobs = job_ids[0:10]

    num_cpu = 3
    # Backup
    schema_name = 'job_backups'
    table_name = 'job_specs'
    run_backup(test_jobs, num_cpu, schema_name, table_name)

    # Update job specs
    # run_update(test_jobs, num_cpu)

# COMMAND ----------


