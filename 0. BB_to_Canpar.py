import os
import pandas as pd
from datetime import datetime
from zeep import Client, Settings
from zeep.exceptions import Fault
from zeep.transports import Transport
from zeep.plugins import HistoryPlugin
import requests
import base64
from lxml import etree

# --- Configuration ---
INPUT_FILE = r"orders.csv"
OUTPUT_DIR = r"."
LABELS_DIR = os.path.join(OUTPUT_DIR, "labels")
XML_RESPONSES_DIR = os.path.join(OUTPUT_DIR, "xml_responses")

# Canpar API Credentials
CANPAR_API_USER = "wafic.alwazzan@visionvation.com"
CANPAR_API_PASSWORD = "Ground291!"
CANPAR_SHIPPER_NUM = "46000041"

# Pickup Address (Shipper)
PICKUP_ADDRESS = {
    "name": "VISIONVATION INC.",
    "street": "133 ROCK FERN WAY",
    "city": "NORTH YORK",
    "province": "ON",
    "postal_code": "M2J4N3",
    "country": "CA",
    "phone": "6474440848"
}

# Canpar WSDL Endpoint
WSDL_URL = "https://canship.canpar.com/canshipws/services/CanshipBusinessService?wsdl"

# Fixed Shipment Details
WEIGHT_LBS = 3.0
DIMENSIONS_INCHES = {"L": 16, "W": 12, "H": 3}
SERVICE_TYPE = "1"  # Canpar Standard Ground
SIGNATURE_REQUIRED = "1"

# ----------------------------------------------------------------------

def get_canpar_client(history_plugin):
    """Initializes and returns the Zeep SOAP Client for Canpar."""
    try:
        for dir_path in [LABELS_DIR, XML_RESPONSES_DIR]:
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
                print(f"Created output directory: {dir_path}")

        session = requests.Session()
        session.verify = True
        transport = Transport(session=session, timeout=60)
        settings = Settings(strict=False, xml_huge_tree=True)
        client = Client(WSDL_URL, settings=settings, transport=transport, plugins=[history_plugin])
        return client
    except Exception as e:
        raise Exception(f"Failed to initialize Canpar SOAP client: {e}")

def save_xml_response(history, order_id, request_type):
    """Saves the raw XML of the last transaction."""
    if not history.last_received:
        return

    # Get the raw XML content
    xml_content = etree.tostring(history.last_received['envelope'], encoding='unicode', pretty_print=True)

    # Save the XML file
    xml_filename = f"{order_id}_{request_type}_response.xml"
    xml_path = os.path.join(XML_RESPONSES_DIR, xml_filename)
    with open(xml_path, 'w') as f:
        f.write(xml_content)
    print(f"   Saved XML response to: {xml_path}")

def create_canpar_shipment(client, history, order_data):
    """Constructs and sends the SOAP request to create a shipment."""
    order_id = str(order_data["Order number"])
    print(f"\n--- Processing Order: {order_id} ---")

    try:
        request_factory = client.type_factory('ns1')
        data_factory = client.type_factory('ns2')

        pickup_addr = data_factory.Address(
            name=PICKUP_ADDRESS['name'], address_line_1=PICKUP_ADDRESS['street'], city=PICKUP_ADDRESS['city'],
            province=PICKUP_ADDRESS['province'], postal_code=PICKUP_ADDRESS['postal_code'], country=PICKUP_ADDRESS['country'],
            phone=PICKUP_ADDRESS['phone']
        )
        delivery_addr = data_factory.Address(
            name=f"{order_data['Shipping address first name']} {order_data['Shipping address last name']}",
            address_line_1=order_data['Shipping address street 1'], address_line_2=order_data.get('Shipping address street 2', ''),
            city=order_data['Shipping address city'], province=order_data['Shipping address state'],
            postal_code=order_data['Shipping address zip'], country='CA', phone=str(order_data.get('Shipping address phone', '')),
            email=order_data.get('Shipping address email', '')
        )
        package = data_factory.Package(
            reported_weight=WEIGHT_LBS, length=DIMENSIONS_INCHES['L'], width=DIMENSIONS_INCHES['W'],
            height=DIMENSIONS_INCHES['H'], declared_value=float(order_data['Total order amount incl. VAT (including shipping charges)'])
        )
        shipment = data_factory.Shipment(
            shipper_num=CANPAR_SHIPPER_NUM, shipping_date=datetime.now(), service_type=SERVICE_TYPE,
            pickup_address=pickup_addr, delivery_address=delivery_addr, packages=[package], order_id=order_id,
            dimention_unit='I', reported_weight_unit='L', nsr=False if SIGNATURE_REQUIRED == "1" else True,
            description=f"{int(order_data['Quantity'])}x {order_data['Offer SKU']}"
        )
        request_data = request_factory.ProcessShipmentRq(
            user_id=CANPAR_API_USER, password=CANPAR_API_PASSWORD, shipment=shipment
        )

        response = client.service.processShipment(request=request_data)
        save_xml_response(history, order_id, "shipment")

        if response and response.error is None:
            shipment_id = response.processShipmentResult.shipment.id
            tracking_num = response.processShipmentResult.shipment.packages[0].barcode
            print(f"✅ Shipment Created! ID: {shipment_id}, Tracking: {tracking_num}")
            return {"status": "SUCCESS", "shipment_id": shipment_id, "tracking_num": tracking_num}
        else:
            error_msg = response.error if response else "Empty response"
            print(f"❌ API Failed for Order {order_id}. Error: {error_msg}")
            return {"status": "FAILED", "error": str(error_msg)}

    except Fault as f:
        save_xml_response(history, order_id, "shipment_fault")
        print(f"❌ SOAP Fault occurred for Order {order_id}: {f.message}")
        return {"status": "SOAP_FAULT", "error": f.message}
    except Exception as e:
        print(f"❌ An unexpected error occurred for Order {order_id}: {e}")
        return {"status": "EXCEPTION", "error": str(e)}

def get_canpar_label(client, history, shipment_id, order_id):
    """Retrieves the shipping label for a given shipment ID."""
    print(f"--- Retrieving Label for Shipment ID: {shipment_id} ---")
    try:
        request_factory = client.type_factory('ns1')
        request_data = request_factory.GetLabelsRq(
            user_id=CANPAR_API_USER, password=CANPAR_API_PASSWORD, id=shipment_id, thermal=False
        )

        response = client.service.getLabels(request=request_data)
        save_xml_response(history, order_id, "label")

        if response and response.error is None and response.labels:
            label_data = response.labels[0]
            label_path = os.path.join(LABELS_DIR, f"{order_id}.pdf")
            with open(label_path, 'wb') as f:
                f.write(base64.b64decode(label_data))
            print(f"   Saved PDF label to: {label_path}")
            return {"status": "SUCCESS", "label_path": label_path}
        else:
            error_msg = response.error if response else "Empty label response"
            print(f"❌ Label Retrieval Failed. Error: {error_msg}")
            return {"status": "FAILED", "error": str(error_msg)}

    except Fault as f:
        save_xml_response(history, order_id, "label_fault")
        print(f"❌ SOAP Fault during label retrieval: {f.message}")
        return {"status": "SOAP_FAULT", "error": f.message}
    except Exception as e:
        print(f"❌ Unexpected error during label retrieval: {e}")
        return {"status": "EXCEPTION", "error": str(e)}

def process_orders():
    """Main function to load orders, create shipments, and get labels."""
    if not os.path.exists(INPUT_FILE):
        print(f"Input file not found: {INPUT_FILE}")
        return 1

    try:
        df = pd.read_csv(INPUT_FILE, encoding="utf-8-sig")
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(1)
        df['Total order amount incl. VAT (including shipping charges)'] = pd.to_numeric(
            df['Total order amount incl. VAT (including shipping charges)'], errors='coerce'
        ).fillna(0.0)

        print(f"Loaded {len(df)} orders for processing.")

        history = HistoryPlugin()
        canpar_client = get_canpar_client(history)

        results = []
        for _, row in df.iterrows():
            order = row.to_dict()
            order_number = str(order["Order number"])

            shipment_result = create_canpar_shipment(canpar_client, history, order)

            tracking_num = "N/A"
            label_status = "FAILED"
            error_details = shipment_result.get('error', 'None')

            if shipment_result["status"] == "SUCCESS":
                tracking_num = shipment_result["tracking_num"]
                label_result = get_canpar_label(canpar_client, history, shipment_result["shipment_id"], order_number)
                if label_result["status"] == "SUCCESS":
                    label_status = "SUCCESS"
                else:
                    error_details = label_result.get('error', 'None')

            order_record = {
                "Order number": order_number, "Customer Name": f"{order['Shipping address first name']} {order['Shipping address last name']}",
                "SKU": order["Offer SKU"], "Quantity": order["Quantity"], "Tracking Number": tracking_num,
                "Shipment API Status": shipment_result.get('status'), "Label API Status": label_status,
                "Error Details": error_details, "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            results.append(order_record)

        print("\n" + "=" * 50)
        print("Processing Complete. Summary:")
        print("=" * 50)

        summary_df = pd.DataFrame(results)
        summary_file = os.path.join(OUTPUT_DIR, "Canpar_Shipment_Summary.xlsx")
        summary_df.to_excel(summary_file, index=False, engine='openpyxl')
        print(f"Summary saved to: {summary_file}")

    except Exception as e:
        print(f"\nFATAL ERROR during processing: {str(e)}")
        return 1

if __name__ == "__main__":
    process_orders()