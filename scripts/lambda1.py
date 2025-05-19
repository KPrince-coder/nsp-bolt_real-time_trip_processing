import json
import boto3
import os
import base64
from decimal import Decimal, InvalidOperation
from datetime import datetime

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb')

# --- Configuration ---
# Get DynamoDB table name from environment variables
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME')
if not DYNAMODB_TABLE_NAME:
    print("Error: DYNAMODB_TABLE_NAME environment variable not set.")
    exit(1)

# --- Helper Function to Process a Single Kinesis Record ---
def process_kinesis_record(record):
    """
    Decodes and parses a single Kinesis record.
    Returns the parsed data (Python dict) formatted for DynamoDB using the RAW# SK prefix,
    or None if parsing fails. Includes improved error handling for numeric conversions.
    """
    try:
        if 'kinesis' not in record or 'data' not in record['kinesis']:
             print(f"Skipping record: Missing 'kinesis' or 'data' key in record structure: {record}")
             return None

        encoded_data = record['kinesis']['data']
        decoded_data = base64.b64decode(encoded_data).decode('utf-8')
        parsed_data = json.loads(decoded_data)

        # --- Extract key fields based on the provided structure ---
        trip_id = parsed_data.get('trip_id')
        data_type = parsed_data.get('data_type') # 'trip_start' or 'trip_end'

        # Determine the primary timestamp field based on data_type
        timestamp_str = None
        if data_type == 'trip_start':
            timestamp_str = parsed_data.get('pickup_datetime')
        elif data_type == 'trip_end':
            timestamp_str = parsed_data.get('dropoff_datetime')

        if not trip_id or not data_type or not timestamp_str:
            print(f"Skipping record due to missing required fields (trip_id, data_type, timestamp): {parsed_data}")
            return None

        # --- Define your DynamoDB Item Structure for RAW events ---
        # PK = trip_id (String)
        # SK = RAW#{data_type}#{timestamp} (String)

        dynamodb_item = {
            'PK': {'S': str(trip_id)}, # Partition Key: trip_id (as String)
            # Sort Key: RAW prefix + data_type + timestamp for uniqueness and state identification
            'SK': {'S': f"RAW#{data_type}#{timestamp_str}"},
            'trip_id': {'S': str(trip_id)}, # Store trip_id as a separate attribute
            'data_type': {'S': data_type}, # Store the event type
            # Add a status field to indicate this is a raw event
            'status': {'S': 'raw'}
        }

        # --- Add all attributes from the parsed data ---
        # Iterate through all key-value pairs in the parsed data and add them
        # to the DynamoDB item, handling types.
        for key, value in parsed_data.items():
            # Skip keys already used for PK, SK, trip_id, data_type, raw_data, status
            if key in ['trip_id', 'data_type']:
                continue

            # Handle numeric values specifically
            if isinstance(value, (int, float)):
                 try:
                     decimal_value = Decimal(str(value))
                     if decimal_value.is_nan() or decimal_value.is_infinite():
                         print(f"Warning: Skipping attribute '{key}' for trip ID '{trip_id}' due to invalid numeric value (NaN/Infinity): {value}")
                         continue # Skip this attribute
                     else:
                         dynamodb_item[key] = {'N': str(decimal_value)} # Store as 'N' type
                 except (InvalidOperation, ValueError, TypeError) as e:
                     print(f"Warning: Could not convert '{key}' value '{value}' to Decimal for trip ID '{trip_id}': {e}. Skipping attribute.")
                     continue # Skip the attribute if conversion fails
            elif isinstance(value, bool):
                 dynamodb_item[key] = {'BOOL': value}
            elif value is None:
                 dynamodb_item[key] = {'NULL': True}
            else: # Assume string or other types that can be stored as String
                 dynamodb_item[key] = {'S': str(value)} # Store as 'S' type

        # Add a timestamp for when this record was processed by Lambda 1
        dynamodb_item['processing_timestamp_lambda1'] = {'S': datetime.utcnow().isoformat()}


        return dynamodb_item # Return the formatted DynamoDB item

    except (json.JSONDecodeError, base64.binascii.Error, KeyError) as e:
        print(f"Error decoding or parsing Kinesis record: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while processing Kinesis record: {e}")
        return None

# --- Helper Function to Write Items to DynamoDB in Batches ---
def batch_write_to_dynamodb(table_name, items):
    """
    Writes a list of items to DynamoDB using batch_write_item.
    Handles the 25 item limit per batch request.
    Implements basic retry for unprocessed items.
    Filters for duplicate PK/SK within the batch before writing.
    """
    if not table_name:
        print("DynamoDB table name not configured. Cannot write items.")
        return

    if not items:
        print("No items to write to DynamoDB.")
        return

    # --- Enhanced duplicate detection and filtering ---
    # Create a dictionary to hold unique items by their PK+SK key
    unique_items_dict = {}
    duplicate_count = 0
    
    for item in items:
        # Extract PK and SK values, ensuring we handle potential missing keys
        pk_val = item.get('PK', {}).get('S')
        sk_val = item.get('SK', {}).get('S')
        
        if pk_val is None or sk_val is None:
            print(f"Warning: Skipping item due to missing PK or SK: {item}")
            continue
            
        # Create a unique key string
        item_key = f"{pk_val}#{sk_val}"
        
        # Only keep the first occurrence of each unique key
        if item_key not in unique_items_dict:
            unique_items_dict[item_key] = item
        else:
            duplicate_count += 1
            print(f"Warning: Detected duplicate item with key: {item_key}")
    
    # Convert dictionary values back to a list
    unique_items = list(unique_items_dict.values())

    if not unique_items:
        print("No valid items to write to DynamoDB after filtering duplicates.")
        return

    print(f"Preparing to write {len(unique_items)} unique items to DynamoDB ({duplicate_count} duplicates filtered out).")

    # Process items in batches of 25 (DynamoDB limit)
    for i in range(0, len(unique_items), 25):
        batch = unique_items[i:i+25]
        retry_count = 0
        max_retries = 3
        items_to_retry = batch
        
        while items_to_retry and retry_count < max_retries:
            try:
                # Prepare the batch write request
                request_items = {
                    table_name: [{'PutRequest': {'Item': item}} for item in items_to_retry]
                }
                
                # Execute the batch write
                response = dynamodb.batch_write_item(RequestItems=request_items)
                
                # Check for unprocessed items
                unprocessed = response.get('UnprocessedItems', {}).get(table_name, [])
                
                if unprocessed:
                    items_to_retry = [item['PutRequest']['Item'] for item in unprocessed]
                    print(f"Batch {i//25}: {len(items_to_retry)} items unprocessed. Retrying... (attempt {retry_count+1}/{max_retries})")
                    retry_count += 1
                else:
                    print(f"Batch {i//25}: Successfully wrote {len(batch)} items to DynamoDB.")
                    break
                    
            except Exception as e:
                print(f"Error during batch_write_item to {table_name} (Batch {i//25}, Retry {retry_count}): {e}")
                
                # Print the item keys that might be causing issues
                for item in items_to_retry:
                    pk = item.get('PK', {}).get('S', 'unknown')
                    sk = item.get('SK', {}).get('S', 'unknown')
                    print(f"  - Key: {pk}#{sk}")
                
                retry_count += 1
                
                # If it's a validation exception related to duplicates, do additional logging
                if "ValidationException" in str(e) and "duplicates" in str(e):
                    # Create a temporary set to check for duplicates within this batch
                    keys_in_batch = set()
                    for item in items_to_retry:
                        pk = item.get('PK', {}).get('S', '')
                        sk = item.get('SK', {}).get('S', '')
                        key = f"{pk}#{sk}"
                        if key in keys_in_batch:
                            print(f"  ERROR: Found duplicate key in current batch: {key}")
                        keys_in_batch.add(key)
                
                # If we've hit max retries, log and continue to next batch
                if retry_count >= max_retries:
                    print(f"Batch {i//25}: Giving up after {max_retries} retries. {len(items_to_retry)} items remain unprocessed.")

# --- Main Lambda Handler ---

def lambda_handler(event, context):
    """
    AWS Lambda handler for processing Kinesis Data Streams batches.
    Parses records, formats them for DynamoDB with RAW# SK prefix,
    filters duplicates, and writes them to DynamoDB.
    Includes basic check for expected Kinesis event structure.
    """
    if 'Records' not in event or not isinstance(event['Records'], list):
        print("Error: Invalid event structure. Expected a Kinesis event with a 'Records' list.")
        print(f"Received event: {json.dumps(event)}")
        return {
            'statusCode': 200,
            'body': json.dumps('Invalid event structure received.')
        }

    print(f"Received Kinesis event with {len(event['Records'])} records.")

    items_for_dynamodb = []

    for record in event['Records']:
        dynamodb_item = process_kinesis_record(record)
        if dynamodb_item:
            items_for_dynamodb.append(dynamodb_item)

    if items_for_dynamodb:
        batch_write_to_dynamodb(DYNAMODB_TABLE_NAME, items_for_dynamodb)
    else:
        print("No valid items to write to DynamoDB from this batch.")

    return {
        'statusCode': 200,
        'body': json.dumps('Kinesis batch processing and initial loading complete!')
    }