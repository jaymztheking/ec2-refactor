import os
import yaml
import datetime as dt

import boto3

from ingestion.db import Source, Target, Postgres, Snowflake
from ingestion.config import pgdict, sfdict
from ingestion.aws import delete_objects_from_s3

PROJECT_ROOT = "C:\\Users\\jmedaugh\\Github\\ec2-refactor"
S3_BUCKET = "ec2-refactor-jmedaugh"

def full_load(source: Source, target: Target, ingest_config: dict) -> dict:
    src_schema = ingest_config['Source']['schema']
    src_table = ingest_config['Source']['table']
    src_audit_cols = ingest_config['Source']['audit_cols']
    tar_schema = ingest_config['Target']['schema']
    tar_table = ingest_config['Target']['table']
    s3_folder = '/'.join([  ingest_config['Environment'],
                            ingest_config['Source']['database'],
                            src_schema,
                            src_table,
                            ''
    ]).upper().replace(' ','_')

    file_count = 1
    load_count = 0

    s3_client = boto3.Session(profile_name='ec2-refactor').client("s3", region_name="us-east-1")
    delete_objects_from_s3(s3_folder, s3_client, S3_BUCKET)

    #target.truncate(tar_schema, tar_table)
    #query = source.build_full_load_query(src_schema, src_table, src_audit_cols)
    #Send Slack
    #source.run_select_query(query)

    
    pass
    

def incremental_load(source: Source, target: Target, ingest_config: dict) -> dict:
    pass


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
                        #Track Start time Ingestion stats

                        #Ingest
                        if tbl_conf['Load Type'] == 'Full Load':
                            full_load(source, target, tbl_conf)
                        elif tbl_conf['Load Type'] == 'Incremental Load':
                            incremental_load(source, target, tbl_conf)
                        else:
                            pass

                        #Track End time Ingestion stats
                        #Send Slack Alert
                        #Validate
                        #Send Validation Slack Alert
                        #Disconnect source and target objects
                        source.disconnect()
                        target.disconnect()
        #Compile ingestion stats into Pandas df
        #Send as Slack message
