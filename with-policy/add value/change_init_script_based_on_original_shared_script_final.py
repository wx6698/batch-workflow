# Databricks notebook source
import copy
import json
import requests

API_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
API_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
AUTH_HEADER = { 'Authorization': f"Bearer {API_TOKEN}" }
policyID = '0013A7EE9D5E83BB'
DEBUG = False
DRY_RUN = True

# COMMAND ----------

def get_job_ids():
    session = requests.Session()
    api_url = f"{API_URL}/api/2.1/jobs/list"
    api_params = { "limit": 25, "offset": 0 }
    response_data = []
    try:
        response = session.get(api_url, headers=AUTH_HEADER, params=api_params)
        response_data = response.json()["jobs"]
        response_length = len(response.json()["jobs"])
        while response.json()["has_more"]:
            api_params["offset"] += 25
            response = session.get(api_url, headers=AUTH_HEADER, params=api_params)
            response_data.append(response.json()["jobs"])
            response_length += len(response.json()["jobs"])
    except requests.exceptions.RequestException:
        pass
    if DEBUG:
        print(response_length)
    return [job_id["job_id"] for job_id in response_data]


def get_job_spec(job_id):
    api_url = f"{API_URL}/api/2.1/jobs/get?job_id={job_id}"
    response = requests.get(api_url, headers=AUTH_HEADER)
    return response.json()


def change_vals(job_spec, conf_key, new_val):
    if isinstance(job_spec, dict):
        new_job_spec = copy.deepcopy(job_spec)
        for key, value in new_job_spec.items():
            if key == conf_key and isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and item.get('dbfs'):
                        item['workspace'] = {"destination": new_val}
                        del item['dbfs']
            else:
                new_job_spec[key] = change_vals(value, conf_key, new_val)
        return new_job_spec
    elif isinstance(job_spec, list):
        new_job_spec = [change_vals(item, conf_key, new_val) for item in job_spec]
        return new_job_spec
    else:
        return job_spec



def update_job_spec(job_id, new_job_spec):
    api_url = f"{API_URL}/api/2.1/jobs/update"
    new_settings = new_job_spec['settings']    
    response = requests.post(api_url, headers=AUTH_HEADER, json={ 'job_id': job_id, 'new_settings': new_settings })
    return response


def update_jobs(job_ids, attribute, new_value):
    jobs_spec = []
    new_jobs_spec = []
    for job_id in job_ids:
        job_spec = get_job_spec(job_id)
        if 'tasks' in job_spec['settings'] and (
        ('job_clusters' in job_spec['settings'] and job_spec['settings']['job_clusters'][0]['new_cluster']['policy_id'] == policyID) or
        'job_clusters' not in job_spec['settings'] and job_spec['settings']['tasks'][0]['new_cluster']['policy_id'] == policyID):
            jobs_spec.append(job_spec)        
            new_job_spec = change_vals(job_spec, attribute, new_value)
            new_jobs_spec.append(new_job_spec)
            if DEBUG:
                print('DEBUG: Job ID: ', job_id)
                # print(new_job_spec)
            if DRY_RUN:
                print('INFO: [Dry-run] Changes to job', job_id)
            else:
                updated = update_job_spec(job_id, new_job_spec)
                if updated.status_code != 200:
                    print('ERROR: Updating job', job_id, updated.text)
                else:
                    print('INFO: Updated job', job_id)
                if DEBUG:
                    print('DEBUG: Updating job', updated.text)

# COMMAND ----------

job_ids = get_job_ids()
attrib_to_change = 'init_scripts'
new_value = '/Workspace/Users/ankit.raj@databricks.com/initscript'
update_jobs(job_ids, attrib_to_change, new_value)

# COMMAND ----------


