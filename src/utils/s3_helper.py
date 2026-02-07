import os
import polars as pl
import boto3
import io
from dotenv import load_dotenv

load_dotenv()

SSO_PROFILE_NAME = os.getenv("AWS_NAME")
SSO_REGION_NAME = os.getenv("AWS_REGION")

def get_select_raw_data_from_AWS(bucket_name, object_key, columns, logger) -> pl.LazyFrame | None:
    try:
        s3_path = f"s3://{bucket_name}/{object_key}"
        logger.info(f"Scanning {s3_path}")

        nppes = pl.scan_csv(
            s3_path,
            storage_options={"profile": SSO_PROFILE_NAME,
                             "region": SSO_REGION_NAME}
        ).select(columns)

        logger.info("CSV scan initialized (lazy execution)")
        return nppes

    except Exception as e:
        logger.error(
            f"Failed to scan s3://{bucket_name}/{object_key}: {e}",
            exc_info=True
        )
        return None

def get_all_raw_data_from_AWS(bucket_name, object_key, logger) -> pl.LazyFrame | None:
    try:
        s3_path = f"s3://{bucket_name}/{object_key}"
        logger.info(f"Scanning {s3_path}")

        nppes = pl.scan_csv(
            s3_path,
            storage_options={"profile": SSO_PROFILE_NAME,
                             "region": SSO_REGION_NAME}
        )

        logger.info("CSV scan initialized (lazy execution)")
        return nppes

    except Exception as e:
        logger.error(
            f"Failed to scan s3://{bucket_name}/{object_key}: {e}",
            exc_info=True
        )
        return None

def get_raw_excel_data_from_AWS(bucket_name, object_key, logger) -> pl.DataFrame | None:
    try:
        s3_path = f"s3://{bucket_name}/{object_key}"
        logger.info(f"Scanning {s3_path}")

        session = boto3.Session(profile_name=SSO_PROFILE_NAME)
        s3_client = session.client('s3')

        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        excel_data = pl.read_excel(io.BytesIO(response['Body'].read()))

        logger.info(
            f'Loaded {object_key} into DataFrame '
            f'({excel_data.height} rows, {excel_data.width} columns)'
        )

        return excel_data

    except Exception as e:
        logger.error(
            f"Failed to scan s3://{bucket_name}/{object_key}: {e}",
            exc_info=True
        )
        return None

def upload_raw_data_to_AWS(data, bucket_name, object_key, logger):
    try:
        logger.info(
            f'Starting S3 upload: bucket={bucket_name}, key={object_key}, '
            f'rows={len(data)}'
        )    
        
        session = boto3.Session(profile_name=SSO_PROFILE_NAME)
        s3_client = session.client('s3')

        csv_buffer = io.StringIO()
        data.write_csv(csv_buffer)
        
        logger.debug(
            f'CSV conversion complete for {object_key}. '
            f'Buffer size: {csv_buffer.tell()} bytes'
        )

        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=csv_buffer.getvalue()
        )

        logger.info(f'Successfully uploaded s3://{bucket_name}/{object_key}')
    except Exception as e:
        logger.error(f'Failed to upload {object_key}: {e}')
