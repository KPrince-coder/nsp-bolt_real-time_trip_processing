import json
import boto3
import os
from datetime import datetime
from decimal import Decimal
import uuid

# Initialize DynamoDB resources
dynamodb = boto3.resource('dynamodb')

# Get DynamoDB table name from environment variables
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME')
if not DYNAMODB_TABLE_NAME:
    print("Error: DYNAMODB_TABLE_NAME environment variable not set.")
    
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# --- Helper class for DynamoDB item serialization ---
class DecimalEncoder(json.JSONEncoder):
    """Helper class to convert Decimal types to numbers during JSON serialization."""
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

# --- Helper Function to Find Counterpart Event ---
def find_counterpart_event(trip_id, data_type):
    """
    Queries DynamoDB to find the counterpart event (start or end) for a given trip_id.
    
    Args:
        trip_id (str): The trip ID to query for
        data_type (str): The data type of the current event ('trip_start' or 'trip_end')
        
    Returns:
        dict: The counterpart event item if found, None otherwise
    """
    # Determine the counterpart data type
    counterpart_data_type = 'trip_end' if data_type == 'trip_start' else 'trip_start'
    
    # Build the SK prefix for the counterpart event
    sk_prefix = f"RAW#{counterpart_data_type}#"
    
    try:
        # Query for counterpart events using PK = trip_id and SK beginning with the counterpart prefix
        response = table.query(
            KeyConditionExpression="PK = :pk AND begins_with(SK, :sk_prefix)",
            ExpressionAttributeValues={
                ":pk": trip_id,
                ":sk_prefix": sk_prefix
            }
        )
        
        if response['Items']:
            # Return the first matching counterpart
            return response['Items'][0]
        else:
            print(f"No counterpart {counterpart_data_type} found for trip_id {trip_id}")
            return None
            
    except Exception as e:
        print(f"Error querying for counterpart event for trip_id {trip_id}: {e}")
        return None

# --- Helper Function to Merge Events and Create Completed Trip ---
def create_completed_trip(start_event, end_event):
    """
    Merges data from trip start and end events into a completed trip record.
    
    Args:
        start_event (dict): The trip start event data
        end_event (dict): The trip end event data
        
    Returns:
        dict: The merged completed trip item for DynamoDB
    """
    trip_id = start_event.get('PK')  # Both events should have the same PK (trip_id)
    
    # Get the dropoff_datetime to use in the SK
    dropoff_datetime = end_event.get('dropoff_datetime', datetime.utcnow().isoformat())
    
    # Create a basic completed trip record
    completed_trip = {
        'PK': trip_id,
        'SK': f"COMPLETED#{dropoff_datetime}",
        'trip_id': trip_id,
        'status': 'completed',
        'processing_timestamp_lambda2': datetime.utcnow().isoformat(),
        'correlation_id': str(uuid.uuid4())  # Add a unique correlation ID for tracing
    }
    
    # Copy all attributes from start event (except certain ones we'll handle differently)
    for key, value in start_event.items():
        if key not in ['PK', 'SK', 'status', 'processing_timestamp_lambda1']:
            completed_trip[key] = value
    
    # Copy all attributes from end event (except certain ones we'll handle differently)
    for key, value in end_event.items():
        if key not in ['PK', 'SK', 'status', 'processing_timestamp_lambda1']:
            # If the key already exists from the start event, prefix it to avoid collision
            if key in completed_trip and key not in ['trip_id', 'data_type']:
                completed_trip[f"end_{key}"] = value
            else:
                completed_trip[key] = value
    
    # Set the data_type to 'completed_trip'
    completed_trip['data_type'] = 'completed_trip'
    
    return completed_trip

# --- Helper Function to Update Event Status ---
def update_event_status(item, new_status):
    """
    Updates the status of an event item in DynamoDB.
    
    Args:
        item (dict): The item to update
        new_status (str): The new status to set
        
    Returns:
        bool: True if update succeeded, False otherwise
    """
    try:
        response = table.update_item(
            Key={
                'PK': item['PK'],
                'SK': item['SK']
            },
            UpdateExpression="SET #status = :new_status, processed_at = :processed_at",
            ExpressionAttributeNames={
                '#status': 'status'
            },
            ExpressionAttributeValues={
                ':new_status': new_status,
                ':processed_at': datetime.utcnow().isoformat()
            },
            ReturnValues="NONE"
        )
        return True
    except Exception as e:
        print(f"Error updating status for item {item['PK']}, {item['SK']}: {e}")
        return False

# --- Helper Function to Put Item with RetryLogic ---
def put_item_with_retry(item, max_retries=3):
    """
    Puts an item into DynamoDB with retry logic.
    
    Args:
        item (dict): The item to put
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        bool: True if put succeeded, False otherwise
    """
    retry_count = 0
    while retry_count < max_retries:
        try:
            response = table.put_item(Item=item)
            return True
        except Exception as e:
            retry_count += 1
            print(f"Error putting item (attempt {retry_count}/{max_retries}): {e}")
            if retry_count >= max_retries:
                print(f"Failed to put item after {max_retries} attempts.")
                return False

# --- Main Lambda Handler ---
def lambda_handler(event, context):
    """
    AWS Lambda handler for processing DynamoDB Stream events.
    Looks for RAW# items, finds their counterparts, and creates COMPLETED items when pairs are found.
    """
    if 'Records' not in event:
        print("Error: Invalid event structure. Expected DynamoDB Stream event with 'Records'.")
        return {
            'statusCode': 200,
            'body': json.dumps('Invalid event structure received.')
        }
    
    print(f"Received DynamoDB Stream event with {len(event['Records'])} records.")
    
    for record in event['Records']:
        # Skip non-INSERT events
        if record['eventName'] != 'INSERT':
            print(f"Skipping non-INSERT event: {record['eventName']}")
            continue
        
        try:
            # Extract the new image of the item (the inserted data)
            if 'NewImage' not in record['dynamodb']:
                print("Skipping record: No 'NewImage' in the DynamoDB record.")
                continue
                
            # Convert DynamoDB format to normal Python dict
            ddb_item = record['dynamodb']['NewImage']
            # Handle unmarshalling (will be done by boto3 for us in most cases)
            
            # Extract key fields for processing
            pk = ddb_item.get('PK', {}).get('S')
            sk = ddb_item.get('SK', {}).get('S', '')
            
            # Only process RAW# events
            if not sk.startswith('RAW#'):
                print(f"Skipping non-RAW event with SK: {sk}")
                continue
                
            # Extract data type ('trip_start' or 'trip_end')
            data_type = ddb_item.get('data_type', {}).get('S')
            if not data_type or data_type not in ['trip_start', 'trip_end']:
                print(f"Skipping record with invalid or missing data_type: {data_type}")
                continue
                
            print(f"Processing {data_type} event for trip_id {pk}")
            
            # Convert DynamoDB NewImage to regular Python dict
            event_item = {}
            for key, value in ddb_item.items():
                # Handle different DynamoDB types
                if 'S' in value:
                    event_item[key] = value['S']
                elif 'N' in value:
                    event_item[key] = Decimal(value['N'])
                elif 'BOOL' in value:
                    event_item[key] = value['BOOL']
                elif 'NULL' in value:
                    event_item[key] = None
                # Add other types as needed
            
            # Find the counterpart event
            counterpart_item = find_counterpart_event(pk, data_type)
            
            if counterpart_item:
                print(f"Found matching counterpart for trip_id {pk}. Creating completed trip.")
                
                # Determine which is start and which is end
                if data_type == 'trip_start':
                    start_event = event_item
                    end_event = counterpart_item
                else:
                    start_event = counterpart_item
                    end_event = event_item
                
                # Create and write the completed trip
                completed_trip = create_completed_trip(start_event, end_event)
                if put_item_with_retry(completed_trip):
                    print(f"Successfully created completed trip record for trip_id {pk}")
                    
                    # Update the status of both raw events to mark them as processed
                    update_event_status(event_item, 'processed_by_matcher')
                    update_event_status(counterpart_item, 'processed_by_matcher')
                else:
                    print(f"Failed to create completed trip record for trip_id {pk}")
            else:
                print(f"No counterpart found yet for trip_id {pk}. Waiting for the matching event.")
                
        except Exception as e:
            print(f"Error processing record: {e}")
            # Continue processing other records even if one fails
            continue
    
    return {
        'statusCode': 200,
        'body': json.dumps('DynamoDB Stream processing complete.', cls=DecimalEncoder)
    }