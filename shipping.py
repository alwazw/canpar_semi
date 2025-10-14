import os
import pandas as pd
from datetime import datetime
from zeep import Client, Settings
from zeep.exceptions import Fault
from zeep.transports import Transport
import requests
import base64
from lxml import etree

import config
import database

# --- Configuration ---
INPUT_FILE = r"orders.csv"
OUTPUT_DIR = r"."
LABELS_DIR = os.path.join(OUTPUT_DIR, "labels")
XML_RESPONSES_DIR = os.path.join(OUTPUT_DIR, "xml_responses")

def get_canpar_client():
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
        client = Client(config.WSDL_URL, settings=settings, transport=transport)
        return client
    except Exception as e:
        raise Exception(f"Failed to initialize Canpar SOAP client: {e}")

def create_canpar_shipment(client, order_data):
    """Constructs and sends the SOAP request to create a shipment."""
    order_id = str(order_data["order_number"])
    print(f"\n--- Processing Order: {order_id} ---")

    try:
        request_factory = client.type_factory('ns1')
        data_factory = client.type_factory('ns2')

        pickup_addr = data_factory.Address(
            name=config.PICKUP_ADDRESS['name'], address_line_1=config.PICKUP_ADDRESS['street'], city=config.PICKUP_ADDRESS['city'],
            province=config.PICKUP_ADDRESS['province'], postal_code=config.PICKUP_ADDRESS['postal_code'], country=config.PICKUP_ADDRESS['country'],
            phone=config.PICKUP_ADDRESS['phone']
        )
        delivery_addr = data_factory.Address(
            name=f"{order_data['first_name']} {order_data['last_name']}",
            address_line_1=order_data['address_line_1'], address_line_2=order_data.get('address_line_2', ''),
            city=order_data['city'], province=order_data['province'],
            postal_code=order_data['postal_code'], country='CA', phone=str(order_data.get('phone', '')),
            email=order_data.get('email', '')
        )
        package = data_factory.Package(
            reported_weight=config.WEIGHT_LBS, length=config.DIMENSIONS_INCHES['L'], width=config.DIMENSIONS_INCHES['W'],
            height=config.DIMENSIONS_INCHES['H'], declared_value=float(order_data['total_amount'])
        )
        shipment = data_factory.Shipment(
            shipper_num=config.CANPAR_SHIPPER_NUM, shipping_date=datetime.now(), service_type=config.SERVICE_TYPE,
            pickup_address=pickup_addr, delivery_address=delivery_addr, packages=[package], order_id=order_id,
            dimention_unit='I', reported_weight_unit='L', nsr=False if config.SIGNATURE_REQUIRED == "1" else True,
            description=f"{int(order_data['quantity'])}x {order_data['product_sku']}"
        )
        request_data = request_factory.ProcessShipmentRq(
            user_id=config.CANPAR_API_USER, password=config.CANPAR_API_PASSWORD, shipment=shipment
        )

        response = client.service.processShipment(request=request_data)

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
        print(f"❌ SOAP Fault occurred for Order {order_id}: {f.message}")
        return {"status": "SOAP_FAULT", "error": f.message}
    except Exception as e:
        print(f"❌ An unexpected error occurred for Order {order_id}: {e}")
        return {"status": "EXCEPTION", "error": str(e)}

def get_canpar_label(client, shipment_id, order_id):
    """Retrieves the shipping label for a given shipment ID."""
    print(f"--- Retrieving Label for Shipment ID: {shipment_id} ---")
    try:
        request_factory = client.type_factory('ns1')
        request_data = request_factory.GetLabelsRq(
            user_id=config.CANPAR_API_USER, password=config.CANPAR_API_PASSWORD, id=shipment_id, thermal=False
        )

        response = client.service.getLabels(request=request_data)

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
        print(f"❌ SOAP Fault during label retrieval: {f.message}")
        return {"status": "SOAP_FAULT", "error": f.message}
    except Exception as e:
        print(f"❌ Unexpected error during label retrieval: {e}")
        return {"status": "EXCEPTION", "error": str(e)}

def import_orders_from_csv():
    """Imports new orders from the CSV file into the database."""
    if not os.path.exists(INPUT_FILE):
        print(f"Input file not found: {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE, encoding="utf-8-sig")
    conn = database.create_connection()

    for _, row in df.iterrows():
        order_number = str(row["Order number"])
        if not database.get_order_by_number(conn, order_number):
            customer_data = (
                row["Shipping address first name"],
                row["Shipping address last name"],
                row["Shipping address email"],
                row["Shipping address phone"],
                row["Shipping address street 1"],
                row.get("Shipping address street 2", ""),
                row["Shipping address city"],
                row["Shipping address state"],
                row["Shipping address zip"],
                row["Shipping address country"]
            )
            customer_id = database.add_customer(conn, customer_data)

            order_data = (
                order_number,
                customer_id,
                row["Offer SKU"],
                row["Quantity"],
                row["Total order amount incl. VAT (including shipping charges)"],
                "NEW"
            )
            database.add_order(conn, order_data)
            print(f"Imported new order: {order_number}")

    conn.close()

def process_new_orders():
    """Processes new orders from the database."""
    conn = database.create_connection()
    cur = conn.cursor()
    cur.execute("SELECT o.id, o.order_number, o.product_sku, o.quantity, o.total_amount, c.first_name, c.last_name, c.email, c.phone, c.address_line_1, c.address_line_2, c.city, c.province, c.postal_code, c.country FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.status = 'NEW'")
    orders_to_process = cur.fetchall()

    if not orders_to_process:
        print("No new orders to process.")
        return

    canpar_client = get_canpar_client()

    for order in orders_to_process:
        order_data = {
            "id": order[0],
            "order_number": order[1],
            "product_sku": order[2],
            "quantity": order[3],
            "total_amount": order[4],
            "first_name": order[5],
            "last_name": order[6],
            "email": order[7],
            "phone": order[8],
            "address_line_1": order[9],
            "address_line_2": order[10],
            "city": order[11],
            "province": order[12],
            "postal_code": order[13],
            "country": order[14]
        }

        shipment_result = create_canpar_shipment(canpar_client, order_data)

        label_path = None
        if shipment_result["status"] == "SUCCESS":
            label_result = get_canpar_label(canpar_client, shipment_result["shipment_id"], order_data["order_number"])
            if label_result["status"] == "SUCCESS":
                label_path = label_result["label_path"]
                order_data["tracking_number"] = shipment_result["tracking_num"]
                send_shipping_confirmation(order_data)
            else:
                shipment_result["status"] = "LABEL_FAILED"
                shipment_result["error"] = label_result.get("error", "None")

        shipment_data = (
            order_data["id"],
            shipment_result.get("shipment_id"),
            shipment_result.get("tracking_num"),
            label_path,
            shipment_result["status"],
            shipment_result.get("error"),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        database.add_shipment(conn, shipment_data)

        # Update order status
        cur.execute("UPDATE orders SET status = ? WHERE id = ?", (shipment_result["status"], order_data["id"]))
        conn.commit()

    conn.close()

def send_shipping_confirmation(order_data):
    """Simulates sending a shipping confirmation email."""
    email_content = f"""
Subject: Your Order {order_data['order_number']} has shipped!

Dear {order_data['first_name']} {order_data['last_name']},

Great news! Your order {order_data['order_number']} has been shipped.
You can track your package using the following tracking number: {order_data['tracking_number']}

Thank you for your purchase!
"""
    with open("shipping_confirmations.log", "a") as f:
        f.write(email_content)
    print(f"Shipping confirmation email simulated for order {order_data['order_number']}.")

if __name__ == "__main__":
    import_orders_from_csv()
    process_new_orders()