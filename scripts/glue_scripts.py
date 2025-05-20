import boto3
import pandas as pd

import json
from datetime import datetime

# --- Configuration ---
# Replace with your DynamoDB table name
DYNAMODB_TABLE_NAME = "trips"

# Replace with your S3 bucket name and desired output file path
S3_BUCKET_NAME = "trips-kpis-buckets-125"

# Create a better organized path with full timestamp for efficient access
# Format: daily_kpis/YEAR/MONTH/DAY/YYYY-MM-DD-HH-MM-SS-daily_trip_kpis.json
now = datetime.now()
year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")
timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")

# Organize in a hierarchical structure for efficient access
S3_OUTPUT_KEY = f"daily_kpis/{year}/{month}/{day}/{timestamp}-daily_trip_kpis.json"

# --- AWS Clients ---
dynamodb = boto3.client("dynamodb")
s3 = boto3.client("s3")


# --- Function to read data from DynamoDB ---
def scan_dynamodb_table(table_name: str) -> pd.DataFrame:
    """
    Scans a DynamoDB table and returns all items as a pandas DataFrame.

    Args:
        table_name (str): Name of the DynamoDB table to scan

    Returns:
        pd.DataFrame: DataFrame containing all items from the DynamoDB table
            or None if there's an error
    """

    print(f"Scanning DynamoDB table: {table_name}")
    items = []
    last_evaluated_key = None

    while True:
        try:
            if last_evaluated_key:
                response = dynamodb.scan(
                    TableName=table_name, ExclusiveStartKey=last_evaluated_key
                )
            else:
                response = dynamodb.scan(TableName=table_name)

            # Convert DynamoDB item format to standard Python dicts
            # This requires knowing the expected data types (S for String, N for Number)
            for item in response.get("Items", []):
                # Example conversion - adjust based on your actual schema
                processed_item = {}
                for key, value in item.items():
                    if "S" in value:
                        processed_item[key] = value["S"]
                    elif "N" in value:
                        processed_item[key] = float(
                            value["N"]
                        )  # Convert Number to float
                    # Add other types (BOOL, L, M, etc.) as needed
                    # For simplicity, we'll focus on S and N for this script's needs

                # Ensure required keys exist, even if None
                processed_item["trip_id"] = processed_item.get("trip_id")
                processed_item["pickup_datetime"] = processed_item.get(
                    "pickup_datetime"
                )
                processed_item["fare_amount"] = processed_item.get("fare_amount")

                items.append(processed_item)

            last_evaluated_key = response.get("LastEvaluatedKey")

            if not last_evaluated_key:
                break  # No more items to scan

            print(f"Scanned {len(items)} items so far. Continuing scan...")

        except Exception as e:
            print(f"Error scanning DynamoDB table {table_name}: {e}")
            # Depending on error handling needs, you might want to exit or retry
            return None  # Return None to indicate failure

    print(f"Finished scanning. Total items retrieved: {len(items)}")
    return items


# --- Main Script Logic ---
if __name__ == "__main__":
    # 1. Read data from DynamoDB
    dynamodb_items = scan_dynamodb_table(DYNAMODB_TABLE_NAME)

    if not dynamodb_items:
        print("No data retrieved from DynamoDB or an error occurred. Exiting.")
        exit()  # Exit the script if no data

    # 2. Load data into Pandas DataFrame
    try:
        df = pd.DataFrame(dynamodb_items)
        print(f"Loaded {len(df)} items into Pandas DataFrame.")
        print("DataFrame head:")
        print(df.head())
        print("DataFrame info:")
        df.info()

        # Ensure required columns exist and have appropriate types
        if (
            "pickup_datetime" not in df.columns
            or "fare_amount" not in df.columns
            or "trip_id" not in df.columns
        ):
            print(
                "Error: Required columns ('pickup_datetime', 'fare_amount', 'trip_id') not found in DataFrame."
            )
            exit()

        # Convert pickup_datetime to datetime objects
        # Use errors='coerce' to turn unparseable dates into NaT (Not a Time)
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")

        # Drop rows where pickup_datetime could not be parsed
        df.dropna(subset=["pickup_datetime"], inplace=True)
        print(f"DataFrame after dropping rows with invalid pickup_datetime: {len(df)}")

        # Convert fare_amount to numeric, coercing errors
        df["fare_amount"] = pd.to_numeric(df["fare_amount"], errors="coerce")

        # Drop rows where fare_amount could not be converted to numeric
        df.dropna(subset=["fare_amount"], inplace=True)
        print(f"DataFrame after dropping rows with invalid fare_amount: {len(df)}")

    except Exception as e:
        print(f"Error creating Pandas DataFrame or processing columns: {e}")
        exit()

    # 3. Calculate KPIs using the user's logic
    try:
        print("Calculating KPIs...")
        df["pickup_date"] = df["pickup_datetime"].dt.date

        # KPI 1: total fare per day
        total_fare_per_day = (
            df.groupby("pickup_date")["fare_amount"].sum().reset_index()
        )
        total_fare_per_day.rename(columns={"fare_amount": "total_fare"}, inplace=True)
        print("Calculated total fare per day.")

        # KPI 2: count of trips
        count_of_trips = df.groupby("pickup_date")["trip_id"].count().reset_index()
        count_of_trips.rename(columns={"trip_id": "trip_count"}, inplace=True)
        print("Calculated count of trips.")

        # KPI 3: average fare
        average_fare = df.groupby("pickup_date")["fare_amount"].mean().reset_index()
        average_fare.rename(columns={"fare_amount": "average_fare"}, inplace=True)
        print("Calculated average fare.")

        # KPI 4: maximum fare
        maximum_fare = df.groupby("pickup_date")["fare_amount"].max().reset_index()
        maximum_fare.rename(columns={"fare_amount": "maximum_fare"}, inplace=True)
        print("Calculated maximum fare.")

        # KPI 5: minimum fare
        minimum_fare = df.groupby("pickup_date")["fare_amount"].min().reset_index()
        minimum_fare.rename(columns={"fare_amount": "minimum_fare"}, inplace=True)
        print("Calculated minimum fare.")

    except Exception as e:
        print(f"Error calculating KPIs: {e}")
        exit()

    # 4. Combine KPIs into a single DataFrame
    try:
        print("Combining KPIs...")
        # Start with one KPI and merge others
        kpi_df = total_fare_per_day
        kpi_df = pd.merge(kpi_df, count_of_trips, on="pickup_date", how="left")
        kpi_df = pd.merge(kpi_df, average_fare, on="pickup_date", how="left")
        kpi_df = pd.merge(kpi_df, maximum_fare, on="pickup_date", how="left")
        kpi_df = pd.merge(kpi_df, minimum_fare, on="pickup_date", how="left")

        print("Combined KPI DataFrame head:")
        print(kpi_df.head())

    except Exception as e:
        print(f"Error combining KPIs: {e}")
        exit()

    # 5. Format the output as JSON instead of CSV
    try:
        print("Formatting output as JSON...")

        # Convert dates to string format for JSON serialization
        kpi_df["pickup_date"] = kpi_df["pickup_date"].astype(str)

        # Structure the JSON as an object with a "daily_kpis" array and enhanced metadata
        now = datetime.now()
        json_structure = {
            "metadata": {
                "report_generated": now.isoformat(),
                "report_timestamp": int(
                    now.timestamp()
                ),  # Unix timestamp for easier sorting/querying
                "report_date": now.strftime("%Y-%m-%d"),
                "report_time": now.strftime("%H:%M:%S"),
                "source_table": DYNAMODB_TABLE_NAME,
                "record_count": len(df),
                "date_range": {
                    "start_date": str(df["pickup_date"].min())
                    if not kpi_df.empty
                    else None,
                    "end_date": str(df["pickup_date"].max())
                    if not kpi_df.empty
                    else None,
                },
                "kpi_count": len(kpi_df) if not kpi_df.empty else 0,
            },
            "daily_kpis": kpi_df.to_dict(orient="records"),
        }

        # Convert to JSON string with indentation for readability
        json_content = json.dumps(json_structure, indent=2)
        print("JSON content generated.")

    except Exception as e:
        print(f"Error formatting output as JSON: {e}")
        exit()

    # 6. Write to S3
    try:
        print(f"Uploading JSON to S3: s3://{S3_BUCKET_NAME}/{S3_OUTPUT_KEY}")

        # Add content type and additional metadata for better organization and discoverability
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_OUTPUT_KEY,
            Body=json_content,
            ContentType="application/json",
            Metadata={
                "report-date": datetime.now().strftime("%Y-%m-%d"),
                "source-table": DYNAMODB_TABLE_NAME,
                "record-count": str(len(df)),
                "report-type": "daily-trip-kpis",
            },
        )

        # If needed, also write a "latest" version to a fixed path for easy access
        latest_key = "daily_kpis/latest/daily_trip_kpis.json"
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=latest_key,
            Body=json_content,
            ContentType="application/json",
            Metadata={
                "report-date": datetime.now().strftime("%Y-%m-%d"),
                "source-table": DYNAMODB_TABLE_NAME,
                "record-count": str(len(df)),
                "report-type": "daily-trip-kpis",
                "original-path": S3_OUTPUT_KEY,
            },
        )

        print(f"Successfully uploaded KPI results to S3 at {S3_OUTPUT_KEY}")
        print(f"Also uploaded a copy to the 'latest' path: {latest_key}")

    except Exception as e:
        print(f"Error uploading to S3: {e}")
        exit()

    print("Glue job finished.")
