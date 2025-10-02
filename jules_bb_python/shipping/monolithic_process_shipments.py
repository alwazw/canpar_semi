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
BEST_BUY_API_KEY = "bfcb96e7-01ba-4470-91b8-005cf820a253"
CANPAR_API_USER = "wafic.alwazzan@visionvation.com"
CANPAR_API_PASSWORD = "Ground291!"
CANPAR_SHIPPER_NUM = "46000041"
PICKUP_ADDRESS = {
    "name": "VISIONVATION INC.", "address_line_1": "133 ROCK FERN WAY", "city": "NORTH YORK",
    "province": "ON", "postal_code": "M2J4N3", "country": "CA", "phone": "6474440848"
}
BEST_BUY_API_URL_BASE = 'https://marketplace.bestbuy.ca/api/orders'
CANPAR_CARRIER_CODE = "CPAR"
WSDL_URL = "https://canship.canpar.com/canshipws/services/CanshipBusinessService?wsdl"
WEIGHT_LBS = 3.0
DIMENSIONS_INCHES = {"L": 16, "W": 12, "H": 3}
SERVICE_TYPE = "1"
SIGNATURE_REQUIRED = "1"

# ==============================================================================
# --- HELPER FUNCTIONS ---
# ==============================================================================
def setup_directories(root_dir):
    """Creates all necessary log and output directories."""
    logs_dir = os.path.join(root_dir, '..', 'logs')
    bb_logs_dir = os.path.join(logs_dir, 'best_buy')
    canpar_logs_dir = os.path.join(logs_dir, 'canpar')
    xml_responses_dir = os.path.join(canpar_logs_dir, 'xml_responses')
    pdf_labels_dir = os.path.join(canpar_logs_dir, 'labels')
    failed_labels_dir = os.path.join(canpar_logs_dir, 'failed_labels')

    for path in [bb_logs_dir, canpar_logs_dir, xml_responses_dir, pdf_labels_dir, failed_labels_dir]:
        os.makedirs(path, exist_ok=True)

    return bb_logs_dir, canpar_logs_dir, xml_responses_dir, pdf_labels_dir, failed_labels_dir

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
    """Saves the raw XML of the last transaction, guaranteed."""
    if not history or not history.last_received:
        print(f"WARNING: No XML response to save for {order_id} ({request_type}).")
        return
    try:
        xml_content = etree.tostring(history.last_received['envelope'], encoding='unicode', pretty_print=True)
        xml_path = os.path.join(xml_dir, f"{order_id}_{request_type}_response.xml")
        with open(xml_path, 'w') as f:
            f.write(xml_content)
        print(f"INFO: Saved XML response to: {xml_path}")
    except Exception as e:
        print(f"CRITICAL: Could not save XML response for {order_id}. Error: {e}")

def is_base64(s):
    """Check if a string is a valid Base64 encoded string."""
    if not isinstance(s, str) or not s:
        return False
    return re.match(r'^[A-Za-z0-9+/=]+$', s)

def create_canpar_shipment(client, history, order, creds, xml_dir):
    order_id = order['order_id']
    print(f"INFO: Attempting to create Canpar shipment for order {order_id}...")
    try:
        req_factory, data_factory = client.type_factory('ns1'), client.type_factory('ns2')
        customer_info = order['customer']
        shipping_addr = customer_info['shipping_address']

        request_data = req_factory.ProcessShipmentRq(
            user_id=creds['user'], password=creds['password'],
            shipment=data_factory.Shipment(
                shipper_num=creds['shipper_num'], shipping_date=datetime.now(), service_type=SERVICE_TYPE,
                pickup_address=data_factory.Address(**creds['pickup_address']),
                delivery_address=data_factory.Address(
                    name=f"{customer_info['firstname']} {customer_info['lastname']}",
                    address_line_1=shipping_addr['street_1'], city=shipping_addr['city'],
                    province=shipping_addr['state'], postal_code=shipping_addr['zip_code'],
                    country=shipping_addr['country_iso_code'], phone=shipping_addr['phone']
                ),
                packages=[data_factory.Package(
                    reported_weight=WEIGHT_LBS, length=DIMENSIONS_INCHES['L'], width=DIMENSIONS_INCHES['W'],
                    height=DIMENSIONS_INCHES['H'], declared_value=float(order['total_price'])
                )],
                order_id=order_id, dimention_unit='I', reported_weight_unit='L',
                nsr=False if SIGNATURE_REQUIRED == "1" else True, description=f"Order {order_id}"
            )
        )
        response = client.service.processShipment(request=request_data)

        if response and response.error is None:
            res = response.processShipmentResult.shipment
            tracking_num = res.packages[0].barcode
            print(f"SUCCESS: Canpar shipment created for {order_id}. Tracking: {tracking_num}")
            return {"status": "SUCCESS", "shipment_id": res.id, "tracking_number": tracking_num}

        error_msg = response.error if response else "Empty or malformed response"
        print(f"ERROR: Canpar API returned an error for order {order_id}: {error_msg}")
        return {"status": "API_ERROR", "error": str(error_msg)}

    except Exception as e:
        print(f"ERROR: An exception occurred while creating shipment for {order_id}: {e}")
        return {"status": "EXCEPTION", "error": str(e)}
    finally:
        save_xml_response(history, order_id, "create_shipment", xml_dir)

def get_canpar_label(client, history, shipment_id, order_id, creds, xml_dir, pdf_dir, failed_dir):
    print(f"INFO: Attempting to retrieve label for shipment ID: {shipment_id}...")
    try:
        request_data = client.type_factory('ns1').GetLabelsRq(user_id=creds['user'], password=creds['password'], id=shipment_id, thermal=False)
        response = client.service.getLabels(request=request_data)

        if response and response.error is None and response.labels:
            label_data = response.labels[0]
            if is_base64(label_data):
                label_path = os.path.join(pdf_dir, f"{order_id}.pdf")
                with open(label_path, 'wb') as f:
                    f.write(base64.b64decode(label_data))
                print(f"SUCCESS: Saved PDF label to: {label_path}")
                return {"status": "SUCCESS"}
            else:
                print(f"ERROR: Label response for {order_id} is not valid Base64.")
                fail_path = os.path.join(failed_dir, f"{order_id}_invalid_label.txt")
                with open(fail_path, 'w') as f:
                    f.write(str(label_data))
                return {"status": "INVALID_LABEL_DATA", "error": "Response was not a valid Base64 string."}

        error_msg = response.error if response else "Empty or malformed label response"
        print(f"ERROR: Label retrieval API returned an error for {order_id}: {error_msg}")
        return {"status": "API_ERROR", "error": str(error_msg)}

    except Exception as e:
        print(f"ERROR: An exception occurred while retrieving label for {order_id}: {e}")
        return {"status": "EXCEPTION", "error": str(e)}
    finally:
        save_xml_response(history, order_id, "get_label", xml_dir)

def update_log_file(log_file, new_entry):
    """Reads, updates, and writes a JSON log file."""
    log_data = []
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            try: log_data = json.load(f)
            except json.JSONDecodeError: pass

    log_data = [entry for entry in log_data if entry['order_id'] != new_entry['order_id']]
    log_data.append(new_entry)

    with open(log_file, 'w') as f:
        json.dump(log_data, f, indent=4)

# ==============================================================================
# --- MAIN WORKFLOW ---
# ==============================================================================
def run_shipping_process():
    print("\n--- Starting Failsafe Monolithic Canpar Shipment Processing Script ---")

    root_dir = os.path.dirname(__file__)
    bb_logs_dir, canpar_logs_dir, xml_dir, pdf_dir, failed_labels_dir = setup_directories(root_dir)
    pending_shipping_file = os.path.join(bb_logs_dir, 'orders_pending_shipping.json')
    canpar_log_file = os.path.join(canpar_logs_dir, 'canpar_shipments_log.json')

    if not os.path.exists(pending_shipping_file):
        print(f"INFO: Pending shipping file not found at {pending_shipping_file}. Nothing to process.")
        return

    with open(pending_shipping_file, 'r') as f:
        try:
            pending_orders = json.load(f)
        except json.JSONDecodeError:
            print(f"ERROR: Could not parse {pending_shipping_file}. Exiting.")
            return

    print(f"INFO: Loaded {len(pending_orders)} orders from pending file.")

    canpar_client, history = get_canpar_client()
    if not canpar_client: return
    canpar_creds = {"user": CANPAR_API_USER, "password": CANPAR_API_PASSWORD, "shipper_num": CANPAR_SHIPPER_NUM, "pickup_address": PICKUP_ADDRESS}

    for order in pending_orders:
        order_id = order['order_id']
        log_entry = {"order_id": order_id, "timestamp": datetime.now().isoformat()}

        shipment_res = create_canpar_shipment(canpar_client, history, order, canpar_creds, xml_dir)
        log_entry['shipment_creation'] = shipment_res

        if shipment_res['status'] == 'SUCCESS':
            shipment_id = shipment_res['shipment_id']
            label_res = get_canpar_label(canpar_client, history, shipment_id, order_id, canpar_creds, xml_dir, pdf_dir, failed_labels_dir)
            log_entry['label_retrieval'] = label_res
        else:
            log_entry['label_retrieval'] = {"status": "SKIPPED", "reason": "Shipment creation failed."}

        update_log_file(canpar_log_file, log_entry)
        print(f"--- Finished processing for order {order_id} ---")

    print("\n--- Monolithic Script Finished ---")

if __name__ == "__main__":
    run_shipping_process()