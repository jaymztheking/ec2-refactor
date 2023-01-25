import boto3

def upload_files_from_disk_to_s3(local_file: str, bucket: str, s3_file: str, s3_client) -> bool:
    try:
        s3_client.upload_file(local_file, bucket, s3_file)
        return True
    except FileNotFoundError:
        print(f"The file {local_file} was not found")
        return False

def delete_objects_from_s3(s3_folder: str, s3_client, bucket: str) -> bool:
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_folder)
        results = response['Contents']
        objects_to_delete = []
        for obj in results:
            objects_to_delete.append({"Key":obj['Key']})
        print(objects_to_delete)
        return True
    except Exception as err:
        print(err)
        return False


def get_dict_from_parameter_store(key: str) -> dict:
    ssm_client = boto3.client("ssm", region_name="us-east-1")
    response = ssm_client.get_parameter(Name=key, WithDecryption=True)
    return response["Parameter"]["Value"]