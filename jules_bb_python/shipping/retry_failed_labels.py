import os
import json
from datetime import datetime
import base64
from lxml import etree
import requests
from zeep import Client, Settings, Transport
from zeep.exceptions import Fault
from zeep.plugins import HistoryPlugin
import re

# ==============================================================================
# --- CONSOLIDATED CONFIGURATION ---
# ==============================================================================
CANPAR_API_USER = "wafic.alwazzan@visionvation.com"
CANPAR_API_PASSWORD = "Ground291!"
WSDL_URL = "https://canship.canpar.com/canshipws/services/CanshipBusinessService?wsdl"

# ==============================================================================
# --- HELPER FUNCTIONS (Duplicated for monolithic execution) ---
# ==============================================================================
def setup_directories(root_dir):
    """Creates all necessary log and output directories."""
    logs_dir = os.path.join(root_dir, '..', 'logs')
    canpar_logs_dir = os.path.join(logs_dir, 'canpar')
    xml_responses_dir = os.path.join(canpar_logs_dir, 'xml_responses')
    pdf_labels_dir = os.path.join(canpar_logs_dir, 'labels')
    failed_labels_dir = os.path.join(canpar_logs_dir, 'failed_labels')

    for path in [canpar_logs_dir, xml_responses_dir, pdf_labels_dir, failed_labels_dir]:
        os.makedirs(path, exist_ok=True)

    return canpar_logs_dir, xml_responses_dir, pdf_labels_dir, failed_labels_dir

def get_canpar_client():
    """Initializes and returns the Zeep SOAP Client and HistoryPlugin."""
    try:
        history = HistoryPlugin()
        transport = Transport(session=requests.Session(), timeout=60)
        client = Client(WSDL_URL, transport=transport, plugins=[history])
        return client, history
    except Exception as e:
        print(f"FATAL: Failed to initialize Canpar SOAP client: {e}")
        return None, None

def save_xml_response(history, order_id, request_type, xml_dir):
    """Saves the raw XML of the last transaction."""
    if not history or not history.last_received:
        print(f"WARNING: No XML response to save for {order_id} ({request_type}).")
        return
    try:
        xml_content = etree.tostring(history.last_received['envelope'], encoding='unicode', pretty_print=True)
        xml_path = os.path.join(xml_dir, f"{order_id}_{request_type}_response.xml")
        with open(xml_path, 'w') as f: f.write(xml_content)
        print(f"INFO: Saved XML response to: {xml_path}")
    except Exception as e:
        print(f"CRITICAL: Could not save XML response for {order_id}. Error: {e}")

def is_base64(s):
    """Check if a string is a valid Base64 encoded string."""
    if not isinstance(s, str) or not s: return False
    return re.match(r'^[A-Za-z0-9+/=]+$', s)

def get_canpar_label(client, history, shipment_id, order_id, creds, xml_dir, pdf_dir, failed_dir):
    """Retrieves and saves the shipping label PDF."""
    print(f"INFO: Retrying label retrieval for shipment ID: {shipment_id}...")
    try:
        request_data = client.type_factory('ns1').GetLabelsRq(user_id=creds['user'], password=creds['password'], id=shipment_id, thermal=False)
        response = client.service.getLabels(request=request_data)

        if response and response.error is None and response.labels:
            label_data = response.labels[0]
            if is_base64(label_data):
                label_path = os.path.join(pdf_dir, f"{order_id}.pdf")
                with open(label_path, 'wb') as f: f.write(base64.b64decode(label_data))
                print(f"SUCCESS: Saved PDF label to: {label_path}")
                return {"status": "SUCCESS"}
            else:
                fail_path = os.path.join(failed_dir, f"{order_id}_invalid_label_retry.txt")
                with open(fail_path, 'w') as f: f.write(str(label_data))
                return {"status": "INVALID_LABEL_DATA", "error": "Response was not a valid Base64 string."}

        error_msg = response.error if response else "Empty label response"
        return {"status": "API_ERROR", "error": str(error_msg)}
    except Exception as e:
        return {"status": "EXCEPTION", "error": str(e)}
    finally:
        save_xml_response(history, order_id, "get_label_retry", xml_dir)

def update_log_file(log_file, updated_entry):
    """Reads a JSON log, updates a specific entry, and writes back."""
    log_data = []
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            try: log_data = json.load(f)
            except json.JSONDecodeError: pass

    entry_found = False
    for i, entry in enumerate(log_data):
        if entry['order_id'] == updated_entry['order_id']:
            log_data[i] = updated_entry
            entry_found = True
            break

    if not entry_found:
        log_data.append(updated_entry)

    with open(log_file, 'w') as f:
        json.dump(log_data, f, indent=4)

# ==============================================================================
# --- RETRY WORKFLOW ---
# ==============================================================================
def run_retry_process():
    print("\n--- Starting Canpar Label Retry Script ---")

    root_dir = os.path.dirname(__file__)
    canpar_logs_dir, xml_dir, pdf_dir, failed_labels_dir = setup_directories(root_dir)
    canpar_log_file = os.path.join(canpar_logs_dir, 'canpar_shipments_log.json')

    if not os.path.exists(canpar_log_file):
        print(f"INFO: Log file not found at {canpar_log_file}. Nothing to retry.")
        return

    with open(canpar_log_file, 'r') as f:
        try:
            log_data = json.load(f)
        except json.JSONDecodeError:
            print(f"ERROR: Log file at {canpar_log_file} is corrupted.")
            return

    orders_to_retry = []
    for entry in log_data:
        if entry.get('shipment_creation', {}).get('status') == 'SUCCESS' and \
           entry.get('label_retrieval', {}).get('status') != 'SUCCESS':
            orders_to_retry.append(entry)

    if not orders_to_retry:
        print("INFO: No failed labels to retry.")
        return

    print(f"INFO: Found {len(orders_to_retry)} labels to retry.")

    canpar_client, history = get_canpar_client()
    if not canpar_client: return
    canpar_creds = {"user": CANPAR_API_USER, "password": CANPAR_API_PASSWORD}

    for entry in orders_to_retry:
        order_id = entry['order_id']
        shipment_id = entry['shipment_creation']['shipment_id']

        print(f"\n--- Retrying Order: {order_id} ---")

        retry_result = get_canpar_label(canpar_client, history, shipment_id, order_id, canpar_creds, xml_dir, pdf_dir, failed_labels_dir)

        entry['label_retrieval'] = retry_result
        entry['last_retry_timestamp'] = datetime.now().isoformat()
        update_log_file(canpar_log_file, entry)
        print(f"INFO: Updated log for order {order_id} with retry result.")

if __name__ == "__main__":
    run_retry_process()