# Databricks notebook source
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
    except requests.exceptions.RequestException:
        pass
    if DEBUG:
        print(response_length)
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

def change_vals(job_spec, conf_key, new_conf_key):
    vals = list(find_vals(job_spec, conf_key))
    print("in change_vals")
    print (vals)
    if vals:
        old_val = vals[0]
        job_spec_str = json.dumps(job_spec)
        job_spec_str = job_spec_str.replace(str(conf_key), str(new_conf_key))
        print (new_conf_key)
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

def update_jobs(job_ids, attribute, new_attribute):
    jobs_spec = []
    new_jobs_spec = []
    for job_id in job_ids:
        job_spec = get_job_spec(job_id)
        jobs_spec.append(job_spec)        
        new_job_spec = change_vals(job_spec, attribute, new_attribute)
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
            print (new_job_spect)
            if updated.status_code != 200:
                print('ERROR: Updating job', job_id, updated.text())
            else:
                print('INFO: Updated job', job_id)
            if DEBUG:
                print('DEBUG: Updating job', updated.text())

def gen_storage_conf_v1 (name):
    conf_spec = []
    auth= f"fs.azure.account.auth.type.{name}.dfs.core.windows.net" 
    provider = f"fs.azure.account.oauth.provider.type.{name}.dfs.core.windows.net"
    id = f"fs.azure.account.oauth2.client.id.{name}.dfs.core.windows.net"
    secret = f"fs.azure.account.oauth2.client.secret.{name}.dfs.core.windows.net"
    endpoint = f"fs.azure.account.oauth2.client.endpoint.{name}.dfs.core.windows.net"
    conf_spec = [auth,provider,id,secret,endpoint]
    return conf_spec


# COMMAND ----------

# vals = list(find_vals(job_spec, conf_key))
#     print(vals)
#     if vals:
#         old_val = vals[0]
#         job_spec_str = json.dumps(job_spec)
#         job_spec_str = job_spec_str.replace(str(old_val), str(new_val))
#         return json.loads(job_spec_str)
#     else:
#         if DEBUG:
#             print('DEBUG: No change in', conf_key)
#         return job_spec



def update_node(job_ids, policy_id, attribute, new_attribute):
  jobs_spec = []
  new_jobs_spec = []
  for job_id in job_ids:
    job_spec = get_job_spec(job_id)
    vals = list(find_vals(job_spec, 'policy_id'))
    if (vals):
      try:
        if(job_spec['settings']['job_clusters'][0]['new_cluster']['policy_id']==policy_id):
          print (job_id)
          vals_driver = list(find_vals(job_spec, attribute))
          if (vals_driver):
            print("find it")
            #print(job_spec['settings']['job_clusters'][0]['new_cluster']['spark_conf'][attribute])
            new_job_spec = change_vals(job_spec, attribute, new_attribute)
            new_jobs_spec.append(new_job_spec)
            if DEBUG:
              print('DEBUG: Job ID: ', job_id)
              print(new_job_spec)
              recursive_compare(job_spec, new_job_spec)
            if DRY_RUN:
              print('INFO: [Dry-run] Changes to job', job_id)
              #recursive_compare(job_spec, new_job_spec)
            else:
              updated = update_job_spec(job_id, new_job_spec)
              if updated.status_code != 200:
                print('ERROR: Updating job', job_id, updated.content)
              else:
                print('INFO: Updated job', job_id)
          else:
            print("do not find it")
            print(job_spec['settings']['job_clusters'][0]['new_cluster']['spark_conf'])

      except NameError:
        print(job_spec)
        pass
      except:
        #print (job_spec)
        if(job_spec['settings']['tasks'][0]['new_cluster']['policy_id']==policy_id):
          print (job_id)
          vals_driver = list(find_vals(job_spec, attribute))
          if (vals-driver):
            print(vals_driver)
            print(job_spec['settings']['job_clusters'][0]['new_cluster'][attribute])
            new_job_spec = change_vals(job_spec, attribute, new_value)
            new_jobs_spec.append(new_job_spec)
            if DEBUG:
              print('DEBUG: Job ID: ', job_id)
              print(new_job_spec)
              recursive_compare(job_spec, new_job_spec)
            if DRY_RUN:
              print('INFO: [Dry-run] Changes to job', job_id)
              #recursive_compare(job_spec, new_job_spec)
            else:
              updated = update_job_spec(job_id, new_job_spec)
              if updated.status_code != 200:
                print('ERROR: Updating job', job_id, updated.content)
              else:
                print('INFO: Updated job', job_id)
          else:
            print(job_spec['settings']['job_clusters'][0]['new_cluster']['node_type_id'])
            print(job_spec['settings']['job_clusters'][0]['new_cluster']['spark_conf'])


# COMMAND ----------

job_ids = get_job_ids()
exist = gen_storage_conf_v1(storage)
  #print(exist)
for old in exist:
  print(old)
  update_node(job_ids, policy_id, old, "spark.hadoop."+old)

# COMMAND ----------

job_ids = get_job_ids()
storage_list = storage.split(",")
for storage in storage_list:
  exist = gen_storage_conf_v1(storage)
  #print(exist)
  for old in exist:
    print(old)
    update_node(job_ids, policy_id, old, "spark.hadoop."+old)

# COMMAND ----------

 

# COMMAND ----------


