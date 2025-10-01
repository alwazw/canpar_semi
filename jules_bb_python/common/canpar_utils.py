import os
from datetime import datetime
from zeep import Client, Settings
from zeep.exceptions import Fault
from zeep.transports import Transport
from zeep.plugins import HistoryPlugin
import requests
import base64
from lxml import etree

# --- Configuration ---
WSDL_URL = "https://canship.canpar.com/canshipws/services/CanshipBusinessService?wsdl"
XML_RESPONSES_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs', 'canpar', 'xml_responses')
PDF_LABELS_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs', 'canpar', 'labels')

# --- Fixed Shipment Details ---
WEIGHT_LBS = 3.0
DIMENSIONS_INCHES = {"L": 16, "W": 12, "H": 3}
SERVICE_TYPE = "1"  # Canpar Standard Ground
SIGNATURE_REQUIRED = "1"

def get_canpar_client():
    """Initializes and returns the Zeep SOAP Client and HistoryPlugin."""
    try:
        for dir_path in [XML_RESPONSES_DIR, PDF_LABELS_DIR]:
            os.makedirs(dir_path, exist_ok=True)

        history = HistoryPlugin()
        session = requests.Session()
        session.verify = True
        transport = Transport(session=session, timeout=60)
        settings = Settings(strict=False, xml_huge_tree=True)
        client = Client(WSDL_URL, settings=settings, transport=transport, plugins=[history])
        return client, history
    except Exception as e:
        print(f"ERROR: Failed to initialize Canpar SOAP client: {e}")
        return None, None

def save_xml_response(history, order_id, request_type):
    """Saves the raw XML of the last transaction."""
    if not history or not history.last_received:
        return
    try:
        xml_content = etree.tostring(history.last_received['envelope'], encoding='unicode', pretty_print=True)
        xml_filename = f"{order_id}_{request_type}_response.xml"
        xml_path = os.path.join(XML_RESPONSES_DIR, xml_filename)
        with open(xml_path, 'w') as f:
            f.write(xml_content)
        print(f"INFO: Saved XML response to: {xml_path}")
    except Exception as e:
        print(f"WARNING: Could not save XML response for {order_id}. Error: {e}")

def create_canpar_shipment(client, history, order_details, canpar_creds):
    """Constructs and sends the SOAP request to create a Canpar shipment."""
    order_id = order_details['order_id']
    print(f"INFO: Creating Canpar shipment for order {order_id}...")

    try:
        request_factory = client.type_factory('ns1')
        data_factory = client.type_factory('ns2')

        pickup_address = canpar_creds['pickup_address']
        pickup_addr_obj = data_factory.Address(
            name=pickup_address['name'], address_line_1=pickup_address['street'], city=pickup_address['city'],
            province=pickup_address['province'], postal_code=pickup_address['postal_code'], country=pickup_address['country'],
            phone=pickup_address['phone']
        )

        shipping_addr = order_details['customer']['shipping_address']
        delivery_addr_obj = data_factory.Address(
            name=f"{shipping_addr['firstname']} {shipping_addr['lastname']}",
            address_line_1=shipping_addr['street_1'], address_line_2=shipping_addr.get('street_2', ''),
            city=shipping_addr['city'], province=shipping_addr['state'],
            postal_code=shipping_addr['zip_code'], country=shipping_addr['country_iso_code'],
            phone=shipping_addr['phone']
        )

        package_obj = data_factory.Package(
            reported_weight=WEIGHT_LBS, length=DIMENSIONS_INCHES['L'], width=DIMENSIONS_INCHES['W'],
            height=DIMENSIONS_INCHES['H'], declared_value=float(order_details['total_price'])
        )

        shipment_obj = data_factory.Shipment(
            shipper_num=canpar_creds['shipper_num'], shipping_date=datetime.now(), service_type=SERVICE_TYPE,
            pickup_address=pickup_addr_obj, delivery_address=delivery_addr_obj, packages=[package_obj],
            order_id=order_id, dimention_unit='I', reported_weight_unit='L',
            nsr=False if SIGNATURE_REQUIRED == "1" else True,
            description=f"Order {order_id}"
        )

        request_data = request_factory.ProcessShipmentRq(
            user_id=canpar_creds['user'], password=canpar_creds['password'], shipment=shipment_obj
        )

        response = client.service.processShipment(request=request_data)
        save_xml_response(history, order_id, "shipment")

        if response and response.error is None:
            shipment_id = response.processShipmentResult.shipment.id
            tracking_num = response.processShipmentResult.shipment.packages[0].barcode
            print(f"SUCCESS: Canpar shipment created for order {order_id}. Tracking: {tracking_num}")
            return {"status": "SUCCESS", "shipment_id": shipment_id, "tracking_number": tracking_num}
        else:
            error_msg = response.error if response else "Empty response"
            print(f"ERROR: Canpar API failed for order {order_id}. Error: {error_msg}")
            return {"status": "FAILED", "error": str(error_msg)}

    except Fault as f:
        save_xml_response(history, order_id, "shipment_fault")
        print(f"ERROR: SOAP Fault for order {order_id}: {f.message}")
        return {"status": "SOAP_FAULT", "error": f.message}
    except Exception as e:
        print(f"ERROR: Unexpected error creating shipment for {order_id}: {e}")
        return {"status": "EXCEPTION", "error": str(e)}

def get_canpar_label(client, history, shipment_id, order_id, canpar_creds):
    """Retrieves and saves the shipping label PDF."""
    print(f"INFO: Retrieving label for Canpar shipment ID: {shipment_id}...")
    try:
        request_factory = client.type_factory('ns1')
        request_data = request_factory.GetLabelsRq(
            user_id=canpar_creds['user'], password=canpar_creds['password'], id=shipment_id, thermal=False
        )

        response = client.service.getLabels(request=request_data)
        save_xml_response(history, order_id, "label")

        if response and response.error is None and response.labels:
            label_data = response.labels[0]
            label_path = os.path.join(PDF_LABELS_DIR, f"{order_id}.pdf")
            with open(label_path, 'wb') as f:
                f.write(base64.b64decode(label_data))
            print(f"SUCCESS: Saved PDF label to: {label_path}")
            return {"status": "SUCCESS"}
        else:
            error_msg = response.error if response else "Empty label response"
            print(f"ERROR: Label retrieval failed for order {order_id}. Error: {error_msg}")
            return {"status": "FAILED", "error": str(error_msg)}

    except Fault as f:
        save_xml_response(history, order_id, "label_fault")
        print(f"ERROR: SOAP Fault during label retrieval for {order_id}: {f.message}")
        return {"status": "SOAP_FAULT", "error": f.message}
    except Exception as e:
        print(f"ERROR: Unexpected error during label retrieval for {order_id}: {e}")
        return {"status": "EXCEPTION", "error": str(e)}