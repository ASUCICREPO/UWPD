import json
import boto3
import pandas as pd
from botocore.exceptions import ClientError
import io
from datetime import datetime
import os

s3 = boto3.client('s3')

def detect_encoding(file_content):
    """Detect the encoding of a file."""
    # Placeholder for encoding detection
    return 'utf-8'

def read_csv_file_from_s3(bucket_name, key):
    """Read a CSV file from S3 with detected encoding."""
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        file_content = response['Body'].read()
        encoding = detect_encoding(file_content)
        df = pd.read_csv(io.BytesIO(file_content), encoding=encoding)
        print(f"File '{key}' read successfully with encoding '{encoding}'.")
        return df
    except Exception as e:
        print(f"Error reading '{key}': {e}")
        return None

def convert_to_datetime(df, columns):
    """Convert specified columns to datetime."""
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        else:
            print(f"Warning: Column '{col}' not found in DataFrame.")
    return df

def process_incidents(dispatched_df, arrival_df):
    """Process dispatched and arrival data to compute time differences."""

    # Define required columns
    incident_number_col = 'Incident Number'
    call_creation_col = 'Call Creation Date and Time'
    call_close_col = 'Call Close Date and Time'
    call_log_time_col = 'Call Log Date-Time'
    incident_type_col = 'Incident Type'
    call_type_col = 'Call Type'  # Renamed from 'Incident Type'
    call_address_col = 'Call Current Address'
    call_common_name_col = 'Call Current Address Common Name'

    # Check for required columns
    required_columns = [
        incident_number_col,
        call_creation_col,
        call_close_col,
        call_log_time_col,
        incident_type_col,
        call_address_col,
        call_common_name_col
    ]

    for col in required_columns:
        if col not in dispatched_df.columns:
            print(f"Warning: Column '{col}' not found in dispatched data.")

    # Rename 'Incident Type' to 'Call Type'
    if incident_type_col in dispatched_df.columns:
        dispatched_df.rename(columns={incident_type_col: call_type_col}, inplace=True)
    else:
        dispatched_df[call_type_col] = 'Unknown'

    # Convert date columns to datetime
    date_columns = [call_creation_col, call_close_col, call_log_time_col]
    dispatched_df = convert_to_datetime(dispatched_df, date_columns)
    arrival_df = convert_to_datetime(arrival_df, [call_log_time_col])

    # Get the first 'Call Log Date-Time' per 'Incident Number' in dispatched_df
    if call_log_time_col in dispatched_df.columns:
        first_dispatched = dispatched_df.sort_values(call_log_time_col).groupby(incident_number_col, as_index=False).first()
    else:
        print(f"Error: '{call_log_time_col}' not found in dispatched data.")
        return None

    # Get the first 'Call Log Date-Time' per 'Incident Number' in arrival_df
    if call_log_time_col in arrival_df.columns:
        first_arrival = arrival_df.sort_values(call_log_time_col).groupby(incident_number_col, as_index=False).first()
    else:
        print(f"Error: '{call_log_time_col}' not found in arrival data.")
        first_arrival = pd.DataFrame(columns=[incident_number_col])

    # Merge the first dispatched and arrival times on 'Incident Number' using a left merge
    merged_df = pd.merge(
        first_dispatched,
        first_arrival[[incident_number_col, call_log_time_col]],
        on=incident_number_col,
        how='left',
        suffixes=('_dispatched', '_arrival')
    )

    # Compute the time difference between arrival and dispatched times
    merged_df['Time Difference'] = merged_df[call_log_time_col + '_arrival'] - merged_df[call_log_time_col + '_dispatched']

    # Proceed with selecting and renaming columns
    result_columns = {
        incident_number_col: 'Incident Number',
        call_creation_col: 'Call Creation Date and Time',
        call_close_col: 'Call Close Date and Time',
        call_type_col: 'Call Type',
        call_log_time_col + '_dispatched': 'Call Log Time Dispatched',
        call_log_time_col + '_arrival': 'Call Log Time Arrival',
        'Time Difference': 'Time Difference',
        call_address_col: 'Call Current Address',
        call_common_name_col: 'Call Current Address Common Name'
    }

    result_df = merged_df[list(result_columns.keys())].rename(columns=result_columns)

    # Compute time difference components
    result_df['Time Difference (seconds)'] = result_df['Time Difference'].dt.total_seconds()
    result_df['Time Difference (minutes)'] = result_df['Time Difference (seconds)'] / 60
    result_df['Time Difference (hours)'] = result_df['Time Difference (seconds)'] / 3600

    # Replace NaT and NaN with 'No Arrival' for display
    result_df['Time Difference'] = result_df['Time Difference'].astype(str).replace('NaT', 'No Arrival')
    result_df['Call Log Time Arrival'] = result_df['Call Log Time Arrival'].astype(str).replace('NaT', 'No Arrival')
    result_df['Time Difference (seconds)'] = result_df['Time Difference (seconds)'].fillna('No Arrival')
    result_df['Time Difference (minutes)'] = result_df['Time Difference (minutes)'].fillna('No Arrival')
    result_df['Time Difference (hours)'] = result_df['Time Difference (hours)'].fillna('No Arrival')

    return result_df

def lambda_handler(event, context):
    # Retrieve bucket name from environment variables or event
    bucket_name = os.environ.get('BUCKET_NAME')
    if not bucket_name:
        return {'statusCode': 500, 'body': 'Bucket name not specified'}

    dispatched_prefix = 'dispatched/'
    arrival_prefix = 'arrival/'
    analysis_prefix = 'analysis/'

    try:
        # Parse the incoming request
        if 'body' not in event or not event['body']:
            return {'statusCode': 400, 'body': 'Invalid request: No body found'}

        body = json.loads(event['body'])

        if 'action' not in body:
            return {'statusCode': 400, 'body': 'Invalid request: No action specified'}

        action = body['action']

        if action == 'upload':
            # Generate presigned URLs for uploading dispatched and arrival files
            dispatched_key = 'dispatched/upload.csv'
            arrival_key = 'arrival/upload.csv'

            try:
                dispatched_presigned_url = s3.generate_presigned_url(
                    'put_object',
                    Params={'Bucket': bucket_name, 'Key': dispatched_key, 'ContentType': 'text/csv'},
                    ExpiresIn=3600
                )
                arrival_presigned_url = s3.generate_presigned_url(
                    'put_object',
                    Params={'Bucket': bucket_name, 'Key': arrival_key, 'ContentType': 'text/csv'},
                    ExpiresIn=3600
                )
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'dispatched_upload_url': dispatched_presigned_url,
                        'arrival_upload_url': arrival_presigned_url
                    })
                }
            except ClientError as e:
                print(f"Error generating presigned URLs: {e}")
                return {'statusCode': 500, 'body': 'Error generating presigned URLs'}

        elif action == 'analysis':
            # Perform analysis
            try:
                # Read the uploaded dispatched and arrival files
                dispatched_key = 'dispatched/upload.csv'
                arrival_key = 'arrival/upload.csv'

                dispatched_df = read_csv_file_from_s3(bucket_name, dispatched_key)
                arrival_df = read_csv_file_from_s3(bucket_name, arrival_key)

                if dispatched_df is None or arrival_df is None:
                    return {'statusCode': 500, 'body': 'Error reading input files'}

                # Process incidents
                result_df = process_incidents(dispatched_df, arrival_df)

                if result_df is None:
                    return {'statusCode': 500, 'body': 'Error processing incidents'}

                # Save result to analysis folder with fixed filename
                output_file_key = f"{analysis_prefix}analysis.csv"
                csv_buffer = io.StringIO()
                result_df.to_csv(csv_buffer, index=False)
                s3.put_object(Bucket=bucket_name, Key=output_file_key, Body=csv_buffer.getvalue())

                # Generate presigned URL for the analysis file
                presigned_url = s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket_name, 'Key': output_file_key},
                    ExpiresIn=3600
                )

                # Generate timestamp for download filename
                analysis_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
                analysis_filename = f"Analysis_{analysis_datetime}.csv"

                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'Success',
                        'presigned_url': presigned_url,
                        'analysis_filename': analysis_filename
                    })
                }
            except Exception as e:
                print(f"Error in analysis action: {e}")
                return {'statusCode': 500, 'body': f'Error processing files: {e}'}

        else:
            return {'statusCode': 400, 'body': f"Invalid action: {action}"}

    except Exception as e:
        print(f"Error in lambda_handler: {e}")
        return {'statusCode': 500, 'body': f'Error processing request: {e}'}
