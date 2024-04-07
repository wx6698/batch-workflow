# Databricks notebook source
# MAGIC %pip install jsonlines
# MAGIC %pip install tqdm

# COMMAND ----------

dbutils.library.restartPython()

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
    api_params = { "limit": 10, "offset": 0 }
    
    try:
        response = session.get(api_url, headers=AUTH_HEADER, params=api_params)
        #response_data = response.json()["jobs"]
        job_ids = []
        for job_id in response.json()["jobs"]:
            #print(job_id["job_id"])
            job_ids.append(job_id["job_id"])
        response_length = len(response.json()["jobs"])
        while response.json()["has_more"]:
            api_params["offset"] += 10
            response = session.get(api_url, headers=AUTH_HEADER, params=api_params)
            for job_id in response.json()["jobs"]:
                #print(job_id["job_id"])
                job_ids.append(job_id["job_id"])
            #response_data.append(response.json()["jobs"])
            response_length += len(response.json()["jobs"])
    except requests.exceptions.RequestException as error:
        print(traceback.format_exc())
        pass
    if DEBUG:
        print(response_length)
    return job_ids
  

def get_job_spec(job_id):
    api_url = f"{API_URL}/api/2.1/jobs/get?job_id={job_id}"
    response = requests.get(api_url, headers=AUTH_HEADER)
    if response.status_code != 200:
          print('ERROR: getting job', job_id, response.status_code,response._content)
    return response.json()



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

def change_vals(job_spec, conf_key, new_val):
    vals = list(find_vals(job_spec, conf_key))
    print(job_spec['job_cluster_key'],"will chage",conf_key,"from: ",vals,"to: ",new_val)
    if vals:
        old_val = vals[0]
        job_spec_str = json.dumps(job_spec)
        job_spec_str = job_spec_str.replace(str(old_val), str(new_val))
        return json.loads(job_spec_str)
    else:
        if DEBUG:
            print('DEBUG: No change in', conf_key)
        return job_spec
    
def add_vals(job_spec, conf_key, new_val):
    vals_conf = list(find_vals(job_spec, conf_key))
    if (len(vals_conf)==0):
        new_conf = {attribute:value}
        print(type(job_spec))
        job_spec["new_cluster"].update(new_conf)
    return job_spec

def update_job_spec(job_id, new_settings):
    api_url = f"{API_URL}/api/2.1/jobs/update"
    
    print({ 'job_id': job_id, 'new_settings': new_settings })
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

def legacy_add_conf(job_spec, conf_key, new_val):
    # if(job_spec['settings']['tasks'][0]['new_cluster']['policy_id']==policy_id):
      #   print (job_id)
      #   vals_driver = list(find_vals(job_spec, attribute))
      #   if (vals-driver):
      #     print(job_spec['settings']['job_clusters'][0]['new_cluster'][attribute])
      #     new_job_spec = change_vals(job_spec, attribute, new_value)
      #     new_jobs_spec.append(new_job_spec)
      #     if DEBUG:
      #       print('DEBUG: Job ID: ', job_id)
      #       print(new_job_spec)
      #       recursive_compare(job_spec, new_job_spec)
      #     if DRY_RUN:
      #       print('INFO: [Dry-run] Changes to job', job_id)
      #       recursive_compare(job_spec, new_job_spec)
      #     else:
      #       updated = update_job_spec(job_id, new_job_spec)
      #       if updated.status_code != 200:
      #         print('ERROR: Updating job', job_id, updated.content)
      #       else:
      #         print('INFO: Updated job', job_id)
      #   else:
      #     new_job_spec = change_vals(job_spec, attribute, new_value)
      #     new_jobs_spec.append(new_job_spec)
      #     if DEBUG:
      #       print('DEBUG: Job ID: ', job_id)
      #       print(new_job_spec)
      #       recursive_compare(job_spec, new_job_spec)
      #     if DRY_RUN:
      #       print('INFO: [Dry-run] Changes to job', job_id)
      #       #recursive_compare(job_spec, new_job_spec)
      #     else:
      #       updated = update_job_spec(job_id, new_job_spec)
      #       if updated.status_code != 200:
      #         print('ERROR: Updating job', job_id, updated.content)
      #       else:
      #          print('INFO: Updated job', job_id)


# COMMAND ----------

import time
def update_node(job_id, policy_id, attribute, new_value):
  jobs_spec = []
  new_jobs_spec = []
  new_settings = {'job_clusters':new_jobs_spec}
  job_spec = get_job_spec(job_id)
  vals = list(find_vals(job_spec, 'policy_id'))
  if (vals and vals[0]==policy_id):
    try:
      for cluster in job_spec['settings']['job_clusters']:
        vals_policy = list(find_vals(cluster, 'policy_id'))
        vals_conf = list(find_vals(cluster, attribute))
        if (vals_policy and vals_policy[0]==policy_id):
          new_cluster_spec = add_vals(cluster, attribute, new_value)
          new_settings['job_clusters'].append(new_cluster_spec)
        else:
          print(job_id," ",cluster['job_cluster_key'],": do not to change due to either the policy not configured or attribute already exist")
          pass

      if DEBUG:
            print('DEBUG: Job ID: ', job_id)
            print(new_jobs_spec)
            recursive_compare(job_spec, new_jobs_spec)
      if DRY_RUN:
            print('INFO: [Dry-run] Changes to job', job_id)
            #recursive_compare(job_spec, new_jobs_spec)
            print("new settings:",new_jobs_spec)
      else:
        updated = update_job_spec(job_id, new_settings)
        print(new_settings)
        if updated.status_code != 200:
          print('ERROR: Updating job', job_id, updated.content)
        else:
          print('INFO: Updated job', job_id,"succeed")

    except NameError as error:
      print(traceback.format_exc())
      pass
    except Exception as error:
      #print ("find an error on: ",job_id,error,vals_policy)
      print(traceback.format_exc())
      pass

  else:
    print(job_id,": clusters do not configure",policy_id)
    time.sleep(0.3)

# COMMAND ----------

# update_node('1023165188303975', policy_id, attribute, value)

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
          futures.append(executor.submit(update_node, *args))
        for future in futures:
          result = future.result()
          pbar.update(1)


# COMMAND ----------


