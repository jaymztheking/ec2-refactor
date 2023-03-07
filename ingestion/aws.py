import os
import json
import boto3

def upload_files_from_disk_to_s3(local_folder: str, bucket: str =None, s3_folder: str =None, s3_client =None) -> bool:
    try:
        for local_file in os.listdir(local_folder):
            s3_file = '/'.join([s3_folder, local_file])
            local_file_path = os.path.join(local_folder, local_file)
            upload_file_from_disk_to_s3(local_file_path, bucket, s3_file, s3_client)
        return True
    except Exception as err:
        print(err)
        return False
        

def upload_file_from_disk_to_s3(local_file: str, bucket: str =None, s3_file: str =None, s3_client =None) -> bool:
    try:
        s3_client.upload_file(local_file, bucket, s3_file)
        return True
    except Exception as err:
        print(err)
        return False

def delete_objects_from_s3(s3_folder: str, s3_client, bucket: str) -> bool:
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_folder)
        results = response['Contents']
        objects_to_delete = []
        for obj in results:
            objects_to_delete.append({"Key":obj['Key']})
        if objects_to_delete:
            response = s3_client.delete_objects(Bucket=bucket, Delete={"Objects": objects_to_delete})
        return True
    except KeyError:
        print('Nothing to delete there')
        return False 
    except Exception as err:
        print(err)
        return False


def get_dict_from_parameter_store(key: str) -> dict:
    try:
        ssm_client = boto3.Session(profile_name='ec2-refactor').client("ssm", region_name="us-east-1")
        response = ssm_client.get_parameter(Name=key, WithDecryption=True)
        return json.loads(response["Parameter"]["Value"])
    except Exception as err:
        print(err)
        return None