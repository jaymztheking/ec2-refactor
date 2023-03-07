import os
import json
import yaml
import datetime as dt
from time import perf_counter

import boto3

from ingestion.db import Target, Snowflake
from ingestion.smartsheet import Smartsheet
from ingestion.config import sfdict
from ingestion.aws import delete_objects_from_s3, upload_files_from_disk_to_s3, get_dict_from_parameter_store
from ingestion.local_files import remove_files_in_local_folder

PROJECT_ROOT = os.getcwd()
S3_BUCKET = "nerd-cf01-ingestion-packages"
NUM_RECORDS_PER_FETCH = 10000
NUM_LOAD_DATA_PER_ITERATION = 1

def data_load(source: Smartsheet, target: Target, ingest_config: dict) -> dict:
    #unpack dict and declare variables for readability
    env = ingest_config['Environment']
    load_type = ingest_config['Load Type'].lower().replace(' ','_')
    
    sheet_name = ingest_config['Source']['sheet name'].lower().replace(' ','_')

    tar_db = ingest_config['Target']['database']
    tar_schema = ingest_config['Target']['schema']
    tar_table = ingest_config['Target']['table']

    local_folder = os.path.join(    PROJECT_ROOT, 
                                    'data',
                                    env,
                                    'smartsheet',
                                    sheet_name
                                )
    s3_folder = '/'.join([  env,
                            'smartsheet',
                            sheet_name
                        ]).upper().replace(' ','_')
    
    target_script = os.path.join(   PROJECT_ROOT,
                                    'ingestion',
                                    'sql',
                                    load_type,
                                    'smartsheet',
                                    sheet_name+'.sql',
                                )

    file_count = 1
    load_count = 0
    start = perf_counter()
    ingestion_stats = {
                        'db_name': 'smartsheet',
                        'schema_name': 'smartsheet_cf01',
                        'table_name': sheet_name,
                        'started_at': dt.datetime.now()                        
                    }

    #Clean Up S3 Bucket before load
    s3_client = boto3.Session.client("s3", region_name="us-east-1")
    delete_objects_from_s3(s3_folder, s3_client, S3_BUCKET)

    #Clean Up Local Folders before load
    remove_files_in_local_folder(local_folder)

    #Create Load Query
    if load_type == 'full_load':
        target.truncate(tar_schema, tar_table)
    else:
        print('Invalid load_type entered')
        raise Exception
    
    #Send Slack

    #Iterate through query results and writing to csvs
    results = True

    try:
        while results:
            if results:
                file_name = 'file'+str(file_count)+'.csv'
                new_data_file = source.download_csv('')
                file_count += 1

                #Run local -> s3 -> target load if num_load reached
                if(len(os.listdir(local_folder)) % NUM_LOAD_DATA_PER_ITERATION == 0):
                    upload_files_from_disk_to_s3(   local_folder, 
                                                    bucket=S3_BUCKET, 
                                                    s3_folder=s3_folder, 
                                                    s3_client=s3_client
                                                )
                    target.run_script(target_script, db=tar_db, schema=tar_schema)
                    #delete_objects_from_s3(s3_folder, s3_client, S3_BUCKET)
                    remove_files_in_local_folder(local_folder)
                    load_count += 1

                    #Send Slack
            
            #Run same local -> s3 -> target for last batch of files
            elif len(os.listdir(local_folder)) > 0:
                upload_files_from_disk_to_s3(   local_folder, 
                                                bucket=S3_BUCKET, 
                                                s3_folder=s3_folder, 
                                                s3_client=s3_client
                                            )
                target.run_script(target_script, db=tar_db, schema=tar_schema)
                #delete_objects_from_s3(s3_folder, s3_client, S3_BUCKET)
                remove_files_in_local_folder(local_folder)
                load_count += 1

                #Send Slack

                break
            else:
                break    
        end = perf_counter()
        ingestion_stats['ended_at'] = dt.datetime.now()
        ingestion_stats['total_time_taken(mins)'] = round((end - start) / 60, 2)
        ingestion_stats['run_status'] = "SUCCESSFUL"
        ingestion_stats['error'] = "NA"
    except Exception as err:
        ingestion_stats["ended_at"] = "NA"
        ingestion_stats["total_time_taken(mins)"] = "NA"
        ingestion_stats["run_status"] = "UNSUCCESSFUL"
        ingestion_stats["error"] = err
    return ingestion_stats
    

#Read Yaml file and iterate over table config dicts
if __name__ == "__main__":
    with open(os.path.join(PROJECT_ROOT, "table-configs.yaml")) as f:
        conf = yaml.load_all(f, Loader=yaml.FullLoader)
        ingestion_stats_lst = list()
        for table in conf:
           if table is not None:
                for tbl, tbl_conf in table.items():
                    ingestion_stats = dict()
                    print(tbl_conf)
                    if tbl_conf['Run']:
                        #unpack dict for readability
                        env = tbl_conf['Environment'].lower()
                        src_platform = tbl_conf['Source']['platform'].lower()
                        tgt_platform = tbl_conf['Target']['platform'].lower()
                        tgt_db = tbl_conf['Target']['database'].lower()
                        sheet_id = tbl_conf['Source']['sheet id'].lower()

                        #Get login details from parameter store
                        src_key = f"/{env}/{src_platform}"
                        tgt_key = f"/{env}/{tgt_platform}/{tgt_db}"
                        src_api_token = get_dict_from_parameter_store(src_key)
                        tgt_parm = get_dict_from_parameter_store(tgt_key)

                        #Create source and target objects
                        if src_platform == 'smartsheet':
                            source = Smartsheet(sheet_id, src_api_token)
                        else:
                            raise Exception('that platform as not been implemented as a source')
                        
                        if tgt_platform == 'snowflake':
                            target = Snowflake(tgt_parm)
                        else:
                            raise Exception('that platform as not been implemented as a target')
                        
                        #Connect source and target objects
                        #source.connect()
                        target.connect()

                        #Ingest
                        ingestion_stats = data_load(source, target, tbl_conf)
                        print(ingestion_stats)
                        
                        #Send Slack Alert

                        #Validate

                        #Send Validation Slack Alert

                        #Disconnect source and target objects
                        #source.disconnect()
                        target.disconnect()
        #Compile ingestion stats into Pandas df
        #Send as Slack message
