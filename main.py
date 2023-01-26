import os
import yaml
import datetime as dt
from time import perf_counter

import boto3

from ingestion.db import Source, Target, Postgres, Snowflake
from ingestion.config import pgdict, sfdict
from ingestion.aws import delete_objects_from_s3, upload_files_from_disk_to_s3
from ingestion.local_files import remove_files_in_local_folder, create_local_data_file

PROJECT_ROOT = os.getcwd() #change to OS path join
S3_BUCKET = "ec2-refactor-jmedaugh"
NUM_RECORDS_PER_FETCH = 1
NUM_LOAD_DATA_PER_ITERATION = 1

def data_load(source: Source, target: Target, ingest_config: dict) -> dict:
    src_schema = ingest_config['Source']['schema']
    src_table = ingest_config['Source']['table']
    src_audit_cols = ingest_config['Source']['audit_cols']
    tar_schema = ingest_config['Target']['schema']
    tar_table = ingest_config['Target']['table']

    local_folder = os.path.join(    PROJECT_ROOT, 
                                    'data',
                                    ingest_config['Environment'],
                                    ingest_config['Source']['database'],
                                    src_schema,
                                    src_table,
                                )
    s3_folder = '/'.join([  ingest_config['Environment'],
                            ingest_config['Source']['database'],
                            src_schema,
                            src_table,
                        ]).upper().replace(' ','_')
    target_script = os.path.join(   PROJECT_ROOT,
                                    'ingestion',
                                    'sql',
                                    ingest_config['Load Type'].lower().replace(' ','_'),
                                    ingest_config['Source']['database'],
                                    src_schema,
                                    src_table+'.sql',
                                )

    file_count = 1
    load_count = 0
    start = perf_counter()
    ingestion_stats = {
                        'db_name': ingest_config['Source']['database'],
                        'schema_name': src_schema,
                        'table_name': src_table,
                        'started_at': dt.datetime.now()                        
                    }

    #Clean Up S3 Bucket before load
    s3_client = boto3.Session(profile_name='ec2-refactor').client("s3", region_name="us-east-1")
    delete_objects_from_s3(s3_folder, s3_client, S3_BUCKET)

    #Clean Up Local Folders before load
    remove_files_in_local_folder(local_folder)

    #Create Load Query
    if ingest_config['Load Type'] == 'Full Load':
        target.truncate(tar_schema, tar_table)
        query = source.build_full_load_query(src_schema, src_table, src_audit_cols)
    elif ingest_config['Load Type'] == 'Incremental Load':
        latest_date = target.get_latest_date_in_table(src_schema, src_table, src_audit_cols)
        query = source.build_incremental_load_query(src_schema, src_table, src_audit_cols, latest_date)
    
    #Send Slack

    #Iterate through query results and writing to csvs
    results = True

    try:
        while results:
            results = source.run_select_query(query, NUM_RECORDS_PER_FETCH)
            if results:
                file_name = 'file'+str(file_count)+'.csv'
                new_data_file = create_local_data_file(results, local_folder, file_name)
                file_count += 1

                #Run local -> s3 -> target load if num_load reached
                if(len(os.listdir(local_folder)) % NUM_LOAD_DATA_PER_ITERATION == 0):
                    upload_files_from_disk_to_s3(   local_folder, 
                                                    bucket=S3_BUCKET, 
                                                    s3_folder=s3_folder, 
                                                    s3_client=s3_client
                                                )
                    target.run_script(target_script)
                    delete_objects_from_s3(s3_folder, s3_client, S3_BUCKET)
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
                target.run_script(target_script)
                delete_objects_from_s3(s3_folder, s3_client, S3_BUCKET)
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
                        
                        #Get login details from parameter store
                        #Create source and target objects
                        source = Postgres(pgdict)
                        target = Snowflake(sfdict)

                        #Connect source and target objects
                        source.connect()
                        target.connect()

                        #Ingest
                        ingestion_stats = data_load(source, target, tbl_conf)
                        print(ingestion_stats)
                        
                        #Send Slack Alert

                        #Validate

                        #Send Validation Slack Alert

                        #Disconnect source and target objects
                        source.disconnect()
                        target.disconnect()
        #Compile ingestion stats into Pandas df
        #Send as Slack message
