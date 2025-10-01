import os
import sys
import json
import requests
from datetime import datetime

# Add project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from common.utils import get_best_buy_api_key, get_canpar_credentials
from common.canpar_utils import get_canpar_client, create_canpar_shipment, get_canpar_label

# --- Configuration ---
LOGS_DIR_BB = os.path.join(os.path.dirname(__file__), '..', 'logs', 'best_buy')
LOGS_DIR_CANPAR = os.path.join(os.path.dirname(__file__), '..', 'logs', 'canpar')
PENDING_SHIPPING_FILE = os.path.join(LOGS_DIR_BB, 'orders_pending_shipping.json')
CANPAR_LOG_FILE = os.path.join(LOGS_DIR_CANPAR, 'canpar_shipments_log.json')
FINAL_ARCHIVE_FILE = os.path.join(LOGS_DIR_BB, 'shipped_and_updated_log.json')
BEST_BUY_API_URL_BASE = 'https://marketplace.bestbuy.ca/api/orders'
CANPAR_CARRIER_CODE = "CPAR" # Canpar's carrier code for Best Buy

def get_orders_to_process():
    """Reads pending orders and filters out those already processed by Canpar."""
    if not os.path.exists(PENDING_SHIPPING_FILE):
        print("INFO: No pending shipping file found. Nothing to process.")
        return []

    with open(PENDING_SHIPPING_FILE, 'r') as f:
        try:
            pending_orders = json.load(f)
        except json.JSONDecodeError:
            print("ERROR: orders_pending_shipping.json is corrupted.")
            return []

    if not os.path.exists(CANPAR_LOG_FILE):
        return pending_orders # No history, process all

    with open(CANPAR_LOG_FILE, 'r') as f:
        try:
            processed_log = json.load(f)
        except json.JSONDecodeError:
            processed_log = []

    processed_order_ids = {entry['order_id'] for entry in processed_log}
    orders_to_process = [order for order in pending_orders if order['order_id'] not in processed_order_ids]

    print(f"INFO: Found {len(pending_orders)} pending orders, {len(orders_to_process)} of which are new.")
    return orders_to_process

def update_bb_tracking(api_key, order_id, tracking_number):
    """Updates the tracking number for an order on Best Buy."""
    url = f"{BEST_BUY_API_URL_BASE}/{order_id}/tracking"
    headers = {'Authorization': api_key, 'Content-Type': 'application/json'}
    payload = {"carrier_code": CANPAR_CARRIER_CODE, "tracking_number": tracking_number}

    print(f"INFO: Updating Best Buy tracking for order {order_id}...")
    try:
        # In a real scenario, we would make the request. Here, we simulate success.
        # response = requests.put(url, headers=headers, json=payload)
        # response.raise_for_status()
        print(f"SUCCESS: (SIMULATED) Updated tracking for order {order_id}.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to update tracking for order {order_id}: {e}")
        return False

def mark_bb_order_as_shipped(api_key, order_id):
    """Marks an order as shipped on Best Buy."""
    url = f"{BEST_BUY_API_URL_BASE}/{order_id}/ship"
    headers = {'Authorization': api_key}

    print(f"INFO: Marking Best Buy order {order_id} as shipped...")
    try:
        # In a real scenario, we would make the request. Here, we simulate success.
        # response = requests.put(url, headers=headers)
        # response.raise_for_status()
        print(f"SUCCESS: (SIMULATED) Marked order {order_id} as shipped.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to mark order {order_id} as shipped: {e}")
        return False

def log_shipment(order_id, tracking_number, canpar_status, bb_status):
    """Logs the result of a shipment processing attempt."""
    log_entry = {
        "order_id": order_id,
        "tracking_number": tracking_number,
        "canpar_status": canpar_status,
        "best_buy_update_status": bb_status,
        "timestamp": datetime.now().isoformat()
    }

    log_data = []
    if os.path.exists(CANPAR_LOG_FILE):
        with open(CANPAR_LOG_FILE, 'r') as f:
            try:
                log_data = json.load(f)
            except json.JSONDecodeError:
                pass
    log_data.append(log_entry)
    with open(CANPAR_LOG_FILE, 'w') as f:
        json.dump(log_data, f, indent=4)

def archive_order(order_details):
    """Appends a successfully processed order to the final archive."""
    archive_data = []
    if os.path.exists(FINAL_ARCHIVE_FILE):
        with open(FINAL_ARCHIVE_FILE, 'r') as f:
            try:
                archive_data = json.load(f)
            except json.JSONDecodeError:
                pass
    archive_data.append(order_details)
    with open(FINAL_ARCHIVE_FILE, 'w') as f:
        json.dump(archive_data, f, indent=4)
    print(f"INFO: Order {order_details['order_id']} has been archived.")

def main():
    print("\n--- Starting Canpar Shipment Processing Script ---")

    bb_api_key = get_best_buy_api_key()
    canpar_creds = get_canpar_credentials()
    if not bb_api_key or not canpar_creds:
        print("ERROR: API keys or credentials not found. Exiting.")
        return

    canpar_client, history = get_canpar_client()
    if not canpar_client:
        print("ERROR: Could not initialize Canpar client. Exiting.")
        return

    orders_to_process = get_orders_to_process()
    for order in orders_to_process:
        order_id = order['order_id']
        print(f"\n--- Processing Order: {order_id} ---")

        # 1. Create Canpar Shipment
        shipment_result = create_canpar_shipment(canpar_client, history, order, canpar_creds)
        if shipment_result['status'] != 'SUCCESS':
            log_shipment(order_id, "N/A", "FAILED", "SKIPPED")
            continue

        # 2. Get Canpar Label
        label_result = get_canpar_label(canpar_client, history, shipment_result['shipment_id'], order_id, canpar_creds)
        if label_result['status'] != 'SUCCESS':
            log_shipment(order_id, shipment_result['tracking_number'], "LABEL_FAILED", "SKIPPED")
            continue

        # 3. Update Best Buy
        tracking_number = shipment_result['tracking_number']
        tracking_updated = update_bb_tracking(bb_api_key, order_id, tracking_number)
        if not tracking_updated:
            log_shipment(order_id, tracking_number, "SUCCESS", "TRACKING_UPDATE_FAILED")
            continue

        shipped_marked = mark_bb_order_as_shipped(bb_api_key, order_id)
        if not shipped_marked:
            log_shipment(order_id, tracking_number, "SUCCESS", "SHIPPED_MARK_FAILED")
            continue

        # 4. Log and Archive
        log_shipment(order_id, tracking_number, "SUCCESS", "SUCCESS")
        archive_order(order)

    print("\n--- Canpar Shipment Processing Script Finished ---")

if __name__ == "__main__":
    main()