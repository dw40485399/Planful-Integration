import datetime
import sys
import json
import requests
import argparse
from baf_lib.commons import Config
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)

#========================================================================
# Save payload to Azure dfs
#========================================================================
def save_json(js, account_url, credential, container, folder_name, file_name) -> None:
    service_client = DataLakeServiceClient(account_url, credential)
    file_system_client = service_client.get_file_system_client(file_system=container)
    directory_client = file_system_client.get_directory_client(folder_name)
    file_client = directory_client.create_file(file_name)
    file_content = json.dumps(js, default=str)
    file_client.upload_data(data=file_content, overwrite=True, timeout=600)
    file_client.flush_data(len(file_content))

#3.7
#========================================================================
# Call Planful endpoint and retrieve GL Planning detail
#========================================================================
if __name__ == '__main__':
    cfg_data, __logger = Config.getConfigAndLogger(__file__)
    __logger.info("----> start")
    __logger.debug("Cmd arguments:" + str(sys.argv))
    fiscal_year = (datetime.datetime.today() - datetime.timedelta(days=1)).year
    month = (datetime.datetime.today() - datetime.timedelta(days=1)).month
    if month/12 <= .25:
        scenario = f"{fiscal_year} Budget"
    elif month/12 <= .50:
        scenario = f"{fiscal_year} 3+9"
    elif month/12 <= .75:
        scenario = f"{fiscal_year} 6+6"
    else:
        scenario = f"{fiscal_year} 9+3"

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--scenario", help="ex. --scenario ''2024 Budget'' ")
        parser.add_argument("--fiscalyear", help="ex. --fiscalyear 2024")

        args = parser.parse_args()
        scenario = args.scenario if args.scenario else scenario
        fiscal_year = args.fiscalyear if args.fiscalyear else fiscal_year

    except SystemExit:
        __logger.error("Incorrect command line params")
        sys.exit(1)

    dl_acct = cfg_data['datalake']['storageacct']
    dl_container = cfg_data['datalake']['container']
    dl_folder = cfg_data['datalake']['folder']
    dl_filename = cfg_data['datalake']['file_name']
    dl_key = cfg_data['datalake']['key']
    insert_values_array = []
    auth = cfg_data['planful']['authorization']
    baseurl = cfg_data['planful']['base_url']

    url = baseurl + f'?Scenario={scenario}&FiscalYear={fiscal_year}'
    try:
        __logger.info(f'Performing GET from {url}')
        payload = {}
        header = {'Authorization': auth}
        resp_req = requests.get(url, headers=header, data=payload)
        resp_handler = json.loads(resp_req.text)
        print(resp_handler)
        __logger.info(f'Response payload received')
    except SystemExit:
        __logger.error("Unable to perform GET")
        sys.exit(1)
    try:
        gl_filename = scenario.replace(" ", "_")+dl_filename
        __logger.info(f'Saving payload to datalake storage {dl_acct}/{dl_container}/{dl_folder}/{gl_filename}')
        save_json(resp_handler, dl_acct, dl_key, dl_container, dl_folder, gl_filename)
    except SystemExit:
        __logger.error(f"Unable to save file {scenario}{dl_filename}")
        sys.exit(1)
    __logger.info("<<---- stop")
