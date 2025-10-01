import os
import json
from datetime import datetime
import base64
from lxml import etree
import requests
from zeep import Client, Settings, Transport
from zeep.exceptions import Fault
from zeep.plugins import HistoryPlugin

# ==============================================================================
# --- CONSOLIDATED CONFIGURATION (Normally in app_config.py) ---
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
# --- CONSOLIDATED CANPAR UTILS (Normally in common/canpar_utils.py) ---
# ==============================================================================
def get_canpar_client(logs_dir):
    """Initializes and returns the Zeep SOAP Client and HistoryPlugin."""
    try:
        xml_responses_dir = os.path.join(logs_dir, 'canpar', 'xml_responses')
        pdf_labels_dir = os.path.join(logs_dir, 'canpar', 'labels')
        for dir_path in [xml_responses_dir, pdf_labels_dir]:
            os.makedirs(dir_path, exist_ok=True)

        history = HistoryPlugin()
        transport = Transport(session=requests.Session(), timeout=60)
        client = Client(WSDL_URL, transport=transport, plugins=[history])
        return client, history, xml_responses_dir, pdf_labels_dir
    except Exception as e:
        print(f"ERROR: Failed to initialize Canpar SOAP client: {e}")
        return None, None, None, None

def save_xml_response(history, order_id, request_type, xml_dir):
    """Saves the raw XML of the last transaction."""
    if not history or not history.last_received: return
    try:
        xml_content = etree.tostring(history.last_received['envelope'], encoding='unicode', pretty_print=True)
        xml_path = os.path.join(xml_dir, f"{order_id}_{request_type}_response.xml")
        with open(xml_path, 'w') as f: f.write(xml_content)
        print(f"INFO: Saved XML response to: {xml_path}")
    except Exception as e:
        print(f"WARNING: Could not save XML response for {order_id}. Error: {e}")

def create_canpar_shipment(client, history, order, creds, xml_dir):
    order_id = order['order_id']
    print(f"INFO: Creating Canpar shipment for order {order_id}...")
    try:
        req_factory, data_factory = client.type_factory('ns1'), client.type_factory('ns2')
        pickup_addr = data_factory.Address(**creds['pickup_address'])
        customer_info = order['customer']
        shipping_addr = customer_info['shipping_address']
        delivery_addr = data_factory.Address(
            name=f"{customer_info['firstname']} {customer_info['lastname']}",
            address_line_1=shipping_addr['street_1'], city=shipping_addr['city'],
            province=shipping_addr['state'], postal_code=shipping_addr['zip_code'],
            country=shipping_addr['country_iso_code'], phone=shipping_addr['phone']
        )
        package = data_factory.Package(
            reported_weight=WEIGHT_LBS, length=DIMENSIONS_INCHES['L'], width=DIMENSIONS_INCHES['W'],
            height=DIMENSIONS_INCHES['H'], declared_value=float(order['total_price'])
        )
        shipment = data_factory.Shipment(
            shipper_num=creds['shipper_num'], shipping_date=datetime.now(), service_type=SERVICE_TYPE,
            pickup_address=pickup_addr, delivery_address=delivery_addr, packages=[package],
            order_id=order_id, dimention_unit='I', reported_weight_unit='L',
            nsr=False if SIGNATURE_REQUIRED == "1" else True, description=f"Order {order_id}"
        )
        request_data = req_factory.ProcessShipmentRq(user_id=creds['user'], password=creds['password'], shipment=shipment)
        response = client.service.processShipment(request=request_data)
        save_xml_response(history, order_id, "shipment", xml_dir)
        if response and response.error is None:
            res = response.processShipmentResult.shipment
            print(f"SUCCESS: Canpar shipment created for {order_id}. Tracking: {res.packages[0].barcode}")
            return {"status": "SUCCESS", "shipment_id": res.id, "tracking_number": res.packages[0].barcode}
        print(f"ERROR: Canpar API failed for order {order_id}. Error: {response.error if response else 'Empty'}")
        return {"status": "FAILED", "error": str(response.error if response else 'Empty')}
    except Exception as e:
        print(f"ERROR: Exception creating shipment for {order_id}: {e}")
        save_xml_response(history, order_id, "shipment_fault", xml_dir)
        return {"status": "EXCEPTION", "error": str(e)}

def get_canpar_label(client, history, shipment_id, order_id, creds, xml_dir, pdf_dir):
    print(f"INFO: Retrieving label for Canpar shipment ID: {shipment_id}...")
    try:
        request_data = client.type_factory('ns1').GetLabelsRq(user_id=creds['user'], password=creds['password'], id=shipment_id, thermal=False)
        response = client.service.getLabels(request=request_data)
        save_xml_response(history, order_id, "label", xml_dir)
        if response and response.error is None and response.labels:
            label_path = os.path.join(pdf_dir, f"{order_id}.pdf")
            with open(label_path, 'wb') as f: f.write(base64.b64decode(response.labels[0]))
            print(f"SUCCESS: Saved PDF label to: {label_path}")
            return {"status": "SUCCESS"}
        print(f"ERROR: Label retrieval failed for {order_id}. Error: {response.error if response else 'Empty'}")
        return {"status": "FAILED", "error": str(response.error if response else 'Empty')}
    except Exception as e:
        print(f"ERROR: Exception retrieving label for {order_id}: {e}")
        save_xml_response(history, order_id, "label_fault", xml_dir)
        return {"status": "EXCEPTION", "error": str(e)}

# ==============================================================================
# --- MAIN PROCESSING SCRIPT (Normally in shipping/process_canpar_shipments.py) ---
# ==============================================================================
def run_shipping_process():
    print("\n--- Starting Monolithic Canpar Shipment Processing Script ---")

    # --- Setup ---
    root_dir = os.path.dirname(__file__)
    logs_dir = os.path.join(root_dir, '..', 'logs')
    bb_logs_dir = os.path.join(logs_dir, 'best_buy')
    canpar_logs_dir = os.path.join(logs_dir, 'canpar')
    os.makedirs(bb_logs_dir, exist_ok=True)
    os.makedirs(canpar_logs_dir, exist_ok=True)

    pending_shipping_file = os.path.join(bb_logs_dir, 'orders_pending_shipping.json')
    canpar_log_file = os.path.join(canpar_logs_dir, 'canpar_shipments_log.json')
    final_archive_file = os.path.join(bb_logs_dir, 'shipped_and_updated_log.json')

    # --- Use In-Memory Test Data ---
    pending_orders_data = [
        {"order_id": "261713880-A", "total_price": 49.99, "customer": {"firstname": "John", "lastname": "Doe", "shipping_address": {"street_1": "123 Main St", "city": "Anytown", "state": "ON", "zip_code": "M1M1M1", "country_iso_code": "CA", "phone": "4165551234"}}},
        {"order_id": "261717383-A", "total_price": 99.99, "customer": {"firstname": "Jane", "lastname": "Smith", "shipping_address": {"street_1": "456 Oak Ave", "city": "Someplace", "state": "QC", "zip_code": "H1H1H1", "country_iso_code": "CA", "phone": "5145555678"}}}
    ]
    with open(pending_shipping_file, 'w') as f: json.dump(pending_orders_data, f, indent=4)
    print(f"INFO: Created dummy pending orders file at {pending_shipping_file}")

    # --- Get Orders to Process ---
    with open(pending_shipping_file, 'r') as f: pending_orders = json.load(f)

    processed_order_ids = set()
    if os.path.exists(canpar_log_file):
        with open(canpar_log_file, 'r') as f:
            try: processed_order_ids = {entry['order_id'] for entry in json.load(f)}
            except json.JSONDecodeError: pass

    orders_to_process = [o for o in pending_orders if o['order_id'] not in processed_order_ids]
    print(f"INFO: Found {len(pending_orders)} pending, {len(orders_to_process)} are new.")

    # --- Initialize API Clients ---
    canpar_creds = {"user": CANPAR_API_USER, "password": CANPAR_API_PASSWORD, "shipper_num": CANPAR_SHIPPER_NUM, "pickup_address": PICKUP_ADDRESS}
    canpar_client, history, xml_dir, pdf_dir = get_canpar_client(logs_dir)
    if not canpar_client: return

    # --- Main Loop ---
    for order in orders_to_process:
        order_id = order['order_id']
        print(f"\n--- Processing Order: {order_id} ---")

        # 1. Canpar Shipment and Label
        shipment_res = create_canpar_shipment(canpar_client, history, order, canpar_creds, xml_dir)
        if shipment_res['status'] != 'SUCCESS': continue

        label_res = get_canpar_label(canpar_client, history, shipment_res['shipment_id'], order_id, canpar_creds, xml_dir, pdf_dir)
        if label_res['status'] != 'SUCCESS': continue

        # 2. Best Buy API Update (Simulated)
        print(f"INFO: (SIMULATED) Updating Best Buy for order {order_id}...")
        print(f"INFO: (SIMULATED) PUT /api/orders/{order_id}/tracking with carrier={CANPAR_CARRIER_CODE}, tracking={shipment_res['tracking_number']}")
        print(f"INFO: (SIMULATED) PUT /api/orders/{order_id}/ship")

        # 3. Log and Archive
        log_entry = {"order_id": order_id, "tracking_number": shipment_res['tracking_number'], "status": "SUCCESS", "timestamp": datetime.now().isoformat()}

        all_logs = []
        if os.path.exists(canpar_log_file):
            with open(canpar_log_file, 'r') as f:
                try: all_logs = json.load(f)
                except json.JSONDecodeError: pass
        all_logs.append(log_entry)
        with open(canpar_log_file, 'w') as f: json.dump(all_logs, f, indent=4)

        archive = []
        if os.path.exists(final_archive_file):
            with open(final_archive_file, 'r') as f:
                try: archive = json.load(f)
                except json.JSONDecodeError: pass
        archive.append(order)
        with open(final_archive_file, 'w') as f: json.dump(archive, f, indent=4)
        print(f"INFO: Order {order_id} processed and archived.")

    print("\n--- Monolithic Canpar Shipment Processing Script Finished ---")

if __name__ == "__main__":
    run_shipping_process()