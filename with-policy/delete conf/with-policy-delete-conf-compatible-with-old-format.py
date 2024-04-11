# Databricks notebook source
# MAGIC %pip install jsonlines
# MAGIC %pip install tqdm

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("policy_id","required")
dbutils.widgets.text("attribute","required")
dbutils.widgets.dropdown("DEBUG","True", ["True", "False"])
dbutils.widgets.dropdown("DRY_RUN","True", ["True", "False"])



# COMMAND ----------

policy_id=dbutils.widgets.get("policy_id")
attribute=dbutils.widgets.get("attribute")
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
        job_spec["new_cluster"].update(new_conf)
    return job_spec

def del_val(job_spec, conf_key):
    vals_conf = list(find_vals(job_spec, conf_key))
    if (len(vals_conf)!=0):
        job_spec["new_cluster"].pop(conf_key)
        print ("new:",job_spec)
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

def legacy_add_conf(job_id, policy_id, attribute):
    jobs_spec = []
    new_jobs_spec = []
    new_settings = {'new_cluster':new_jobs_spec}
    job_spec = get_job_spec(job_id)
    val_policy = list(find_vals(job_spec, 'policy_id'))
    if (val_policy and val_policy[0]==policy_id):
        try:
            task = job_spec['settings']['tasks'][0]#
            vals_conf = list(find_vals(task, attribute))
            if len(vals_conf) !=0:
                # new_conf = {attribute:value}
                # job_spec["new_cluster"].update(new_conf)
                new_task_spec = del_val(task, attribute)
                
                if DEBUG:
                    print('DEBUG: Job ID: ', job_id)
                    print(new_jobs_spec)
                    recursive_compare(job_spec, new_jobs_spec)
                if DRY_RUN:
                    print('INFO: [Dry-run] Changes to job', job_id)
                    #recursive_compare(job_spec, new_jobs_spec)
                    print("new settings:",new_jobs_spec)
                else:
                    updated = update_job_spec(job_id, {'new_cluster':new_task_spec['new_cluster']})
                    if updated.status_code != 200:
                        print('ERROR: Updating job', job_id, updated.content)
                    else:
                        print('INFO: Updated job', job_id,"succeed")
            else:
                print(job_id," already has the attribute in place ")
                pass

            

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

def del_conf(job_id, policy_id, attribute):
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
          new_cluster_spec = del_val(cluster, attribute)
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

def update_job(job_id, policy_id, attribute):
    job_spec = get_job_spec(job_id)
    job_clusters = list(find_vals(job_spec, "job_clusters"))
    existing_cluster_id = list(find_vals(job_spec, "existing_cluster_id"))
    if len(existing_cluster_id)!=0 :
        print(job_id,"use exist cluster",existing_cluster_id)
    elif len(job_clusters)!=0 :
        print(job_id,"is in new formate")
        del_conf(job_id, policy_id, attribute)
    else:
        print(job_id,"is in legacy formate")
        legacy_add_conf(job_id, policy_id, attribute)

# COMMAND ----------

update_job('877893488967046',policy_id,attribute)

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
          args = [job_id, policy_id, attribute]
          futures.append(executor.submit(update_job, *args))
        for future in futures:
          result = future.result()
          pbar.update(1)


# COMMAND ----------


