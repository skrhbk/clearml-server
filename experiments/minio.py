import boto3

if __name__ == "__main__":
    from config import config
    minio_conf = config.get("fileserver.minio")

    # Replace these with your MinIO server details
    minio_endpoint = minio_conf.minio_endpoint
    access_key = minio_conf.access_key
    secret_key = minio_conf.secret_key
    bucket_name = minio_conf.bucket_name

    # Create an S3 client for MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        # region_name='us-east-1',  # Replace with your desired region
    )

    # List all buckets in your MinIO server
    response = s3_client.list_buckets()
    buckets = response['Buckets']
    print("Buckets:")
    for bucket in buckets:
        print(f"- {bucket['Name']}")

    # Upload a file to the MinIO bucket
    file_name = 'example.txt'
    file_content = b'This is an example file content.'

    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=file_content,
    )

    print(f"{file_name} uploaded to {bucket_name}.")

    # Download a file from the MinIO bucket
    downloaded_file_name = 'downloaded_example.txt'

    with open(downloaded_file_name, 'wb') as file:
        s3_client.download_fileobj(bucket_name, file_name, file)

    print(f"{file_name} downloaded as {downloaded_file_name}.")

    # Delete the uploaded file
    s3_client.delete_object(Bucket=bucket_name, Key=file_name)
    print(f"{file_name} deleted from {bucket_name}.")
