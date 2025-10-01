import os
import pandas as pd
import json
from datetime import datetime

# --- Configuration ---
INPUT_FILE = "Canpar_Shipment_Summary.xlsx"
OUTPUT_FILE = "Bestbuy_Import.csv"
ALL_SHIPMENTS_XLSX = "All_Bestbuy_Imports.xlsx"
ALL_SHIPMENTS_JSON = "All_Bestbuy_Imports.json"
CARRIER_NAME = "Canpar"
CARRIER_URL_TEMPLATE = "https://www.canpar.com/en/track/track.htm?i={}"

# ----------------------------------------------------------------------

def main():
    """
    Main function to process the Canpar shipment summary and generate
    the Best Buy import file.
    """
    # 1. Check if input file exists
    if not os.path.exists(INPUT_FILE):
        print(f"Error: The input file '{INPUT_FILE}' was not found.")
        return

    # 2. Delete previous Bestbuy_Import.csv if it exists
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        print(f"Removed existing output file: {OUTPUT_FILE}")

    # 3. Load the Canpar_Shipment_Summary.xlsx and validate
    try:
        df = pd.read_excel(INPUT_FILE)
    except Exception as e:
        print(f"Error reading Excel file '{INPUT_FILE}': {e}")
        return

    # Validate required columns
    required_columns = ["Order number", "Tracking Number", "Shipment API Status", "Label API Status"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Error: The input file is missing required columns: {', '.join(missing_columns)}")
        return

    # 4. Filter for successfully processed orders
    successful_df = df[
        (df["Shipment API Status"] == "SUCCESS") &
        (df["Label API Status"] == "SUCCESS") &
        (df["Tracking Number"] != "N/A")
    ].copy()

    if successful_df.empty:
        print("No new successful shipments to process.")
        return

    print(f"Found {len(successful_df)} new successful shipments to process.")

    # 5. Create history files if they don't exist
    if not os.path.exists(ALL_SHIPMENTS_XLSX):
        pd.DataFrame().to_excel(ALL_SHIPMENTS_XLSX, index=False)
        print(f"Created history file: {ALL_SHIPMENTS_XLSX}")

    if not os.path.exists(ALL_SHIPMENTS_JSON):
        with open(ALL_SHIPMENTS_JSON, "w") as f:
            json.dump([], f)
        print(f"Created history file: {ALL_SHIPMENTS_JSON}")

    # 6. Map fields for the Best Buy import
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    bestbuy_import_df = pd.DataFrame({
        "order-id": successful_df["Order number"],
        "carrier-name": CARRIER_NAME,
        "carrier-url": successful_df["Tracking Number"].apply(lambda x: CARRIER_URL_TEMPLATE.format(x)),
        "tracking-number": successful_df["Tracking Number"],
        "datetime-created": current_datetime
    })

    # 7. Filter out records that already exist in the JSON history
    try:
        if os.path.getsize(ALL_SHIPMENTS_JSON) > 0:
            with open(ALL_SHIPMENTS_JSON, "r") as f:
                existing_records = json.load(f)
        else:
            existing_records = []
    except (IOError, json.JSONDecodeError) as e:
        print(f"Warning: Could not read history from {ALL_SHIPMENTS_JSON}. Treating as empty. Error: {e}")
        existing_records = []

    existing_order_ids = {str(record["order-id"]) for record in existing_records}

    # Keep only the records that are not already in the history file
    new_records_df = bestbuy_import_df[~bestbuy_import_df["order-id"].astype(str).isin(existing_order_ids)].copy()

    if new_records_df.empty:
        print("No new shipments to add to Best Buy import file. All successful shipments have been processed previously.")
        return

    print(f"Found {len(new_records_df)} shipments to be added to the import file.")

    # 8. Append new records to history files
    # Append to XLSX
    try:
        all_shipments_df = pd.read_excel(ALL_SHIPMENTS_XLSX, engine="openpyxl")
        updated_shipments_df = pd.concat([all_shipments_df, new_records_df], ignore_index=True)
        updated_shipments_df.to_excel(ALL_SHIPMENTS_XLSX, index=False, engine="openpyxl")
        print(f"Appended {len(new_records_df)} records to {ALL_SHIPMENTS_XLSX}")
    except Exception as e:
        print(f"Error updating '{ALL_SHIPMENTS_XLSX}': {e}")

    # Append to JSON
    new_records_list = new_records_df.to_dict(orient="records")
    existing_records.extend(new_records_list)
    with open(ALL_SHIPMENTS_JSON, "w") as f:
        json.dump(existing_records, f, indent=4)
    print(f"Appended {len(new_records_df)} records to {ALL_SHIPMENTS_JSON}")

    # 9. Save the new records to Bestbuy_Import.csv
    new_records_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Successfully created Bestbuy import file: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()