import os
import pandas as pd
from datetime import datetime, timedelta
import json
from zeep import Client, Settings
from zeep.exceptions import Fault
from zeep.transports import Transport
import requests
import base64

# --- Configuration ---
# NOTE: Update these paths to match your actual system
INPUT_FILE = r"C:\Users\Admin\OneDrive\Bestbuy\shipping\Canpar\orders.csv"
OUTPUT_DIR = r"C:\Users\Admin\OneDrive\Bestbuy\shipping\Canpar"
LABELS_DIR = os.path.join(OUTPUT_DIR, "labels")

# Canpar API Credentials
CANPAR_API_USER = "wafic.alwazzan@visionvation.com"
CANPAR_API_PASSWORD = "Ground291!"
CANPAR_SHIPPER_NUM = "46000041"

# Canpar WSDL Endpoint for Shipment Creation
# NOTE: Using the production URL now
WSDL_URL = "https://canship.canpar.com/canshipws/services/CanshipBusinessService?wsdl"

# Fixed Shipment Details (As per your requirements)
WEIGHT_LBS = 3.0  # Total Weight in lbs
DIMENSIONS_INCHES = {"L": 16, "W": 12, "H": 3} # Package dimensions in inches (L x W x H)
SERVICE_TYPE = "1" # Canpar Standard Ground (commonly 1 or 01)
SIGNATURE_REQUIRED = "1" # User requirement: '1' for Signature Required (SR)
# Canpar uses '1' for SR. If you want 'No Signature Required' use '0'
# The 'signature' field expects a value like 'SR' for Signature Required.
SIGNATURE_OPTIONS = {'0': '', '1': 'SR'} 

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
# ----------------------------------------------------------------------

def get_canpar_client():
    """Initializes and returns the Zeep SOAP Client for Canpar."""
    try:
        if not os.path.exists(LABELS_DIR):
            os.makedirs(LABELS_DIR)
            print(f"Created output directory: {LABELS_DIR}")

        transport = Transport(timeout=60)
        # Disable strict mode to handle minor WSDL/XML discrepancies common in enterprise systems
        settings = Settings(strict=False, xml_huge_tree=True)
        # client = Client(WSDL_URL, settings=settings, transport=transport)
        
        # Explicitly defining a session to possibly improve SSL/HTTPS stability
        session = requests.Session()
        session.verify = True # Ensures SSL certificate verification
        transport = Transport(session=session, timeout=60)
        client = Client(WSDL_URL, settings=settings, transport=transport)
        
        return client
    except Exception as e:
        raise Exception(f"Failed to initialize Canpar SOAP client: {e}")

def create_canpar_shipment(client, order_data):
    """
    Constructs and sends the SOAP request to create a shipment for a single order.
    The request is built as a single ProcessShipmentRq object.
    """
    # Ensure Order number is treated as a string for use as an ID/Reference
    order_id = str(order_data["Order number"])
    print(f"\n--- Processing Order: {order_id} ---")

    try:
        # Shipping date (Tomorrow in YYYYMMDD format)
        shipping_date_str = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")

        # --- Create Complex Type Objects using client.get_type() ---

        # 1. Get the structure for the main request object: ProcessShipmentRq
        # NOTE: Often the WSDL type is ProcessShipmentRq, but sometimes just ProcessShipment
        # We will try ProcessShipmentRq first, but use the factory to ensure we find the correct type
        RequestType = client.get_type('ns0:ProcessShipmentRq') # ns0 is usually the first namespace
        AddressType = client.get_type('ns0:Address')
        PieceType = client.get_type('ns0:Piece')
        ShipmentType = client.get_type('ns0:Shipment')
        
        # 2. Prepare Shipper/Pickup Information
        shipper_obj = AddressType()
        shipper_obj.attention = PICKUP_ADDRESS.get('name', '')
        shipper_obj.name = PICKUP_ADDRESS.get('name', '')
        shipper_obj.address_line_1 = PICKUP_ADDRESS.get('street', '')
        shipper_obj.city = PICKUP_ADDRESS.get('city', '')
        shipper_obj.province = PICKUP_ADDRESS.get('province', '')
        shipper_obj.postal_code = PICKUP_ADDRESS.get('postal_code', '')
        shipper_obj.country = PICKUP_ADDRESS.get('country', '')
        shipper_obj.phone = PICKUP_ADDRESS.get('phone', '')
        shipper_obj.residential = '0'

        # 3. Prepare Consignee/Delivery Information
        delivery_attention = f"{int(order_data['Quantity'])}x {order_data['Offer SKU']}"
        
        consignee_obj = AddressType()
        consignee_obj.id = order_id
        consignee_obj.attention = delivery_attention
        consignee_obj.name = f"{order_data['Shipping address first name']} {order_data['Shipping address last name']}"
        consignee_obj.address_line_1 = order_data['Shipping address street 1']
        consignee_obj.address_line_2 = order_data['Shipping address street 2'] if pd.notna(order_data['Shipping address street 2']) else ''
        # Removed address_line_3 as it might cause issues if not strictly necessary
        consignee_obj.city = order_data['Shipping address city']
        consignee_obj.province = order_data['Shipping address state']
        consignee_obj.postal_code = order_data['Shipping address zip']
        consignee_obj.country = 'CA'
        consignee_obj.phone = str(order_data['Shipping address phone']) if pd.notna(order_data['Shipping address phone']) else ''
        consignee_obj.email = order_data['Shipping address email'] if pd.notna(order_data['Shipping address email']) else ''
        consignee_obj.residential = '0'

        # 4. Prepare Package/Piece Information 
        pieces_list = []
        piece_obj = PieceType()
        piece_obj.declared_value = float(order_data['Total order amount incl. VAT (including shipping charges)'])
        piece_obj.length = DIMENSIONS_INCHES['L']
        piece_obj.width = DIMENSIONS_INCHES['W']
        piece_obj.height = DIMENSIONS_INCHES['H']
        piece_obj.weight = WEIGHT_LBS
        piece_obj.weight_unit = 'L' # L for Lbs
        piece_obj.dim_unit = 'I' # I for Inches
        piece_obj.reference = order_id
        pieces_list.append(piece_obj)

        # 5. Prepare Shipment Details
        shipment_details_obj = ShipmentType()
        shipment_details_obj.reference = order_id
        shipment_details_obj.service_type = SERVICE_TYPE
        shipment_details_obj.number_of_pieces = 1 # One physical box per order
        shipment_details_obj.total_weight = WEIGHT_LBS
        shipment_details_obj.total_declared_value = float(order_data['Total order amount incl. VAT (including shipping charges)'])
        shipment_details_obj.shipping_date = shipping_date_str # Format: YYYYMMDD
        shipment_details_obj.signature = SIGNATURE_OPTIONS.get(SIGNATURE_REQUIRED, '') # 'SR' for Signature Required
        shipment_details_obj.dangerous_goods = '0'
        shipment_details_obj.premium = 'N'
        shipment_details_obj.label_format_type = '6' # 6 = PDF
        # Ensure all mandatory fields are present, even if empty
        shipment_details_obj.cod_type = ''
        shipment_details_obj.total_tax = 0.0
        shipment_details_obj.total_duty = 0.0

        # 6. Assemble the final ProcessShipmentRq request object
        process_request = RequestType(
            user_id=CANPAR_API_USER,
            password=CANPAR_API_PASSWORD,
            shipper_num=CANPAR_SHIPPER_NUM,
            shipper=shipper_obj,
            consignee=consignee_obj,
            shipment=shipment_details_obj,
            pieces=pieces_list
        )
        
        # 7. Call the API with the single 'request' argument
        shipment_creation = client.service.processShipment
        response = shipment_creation(request=process_request)
        
        # Convert the zeep object to a standard Python dictionary
        response_dict = client.factory.to_dict(response)
        
        if response_dict and response_dict.get('success') == '1':
            tracking_num = response_dict.get('tracking_num', 'N/A')
            print(f"✅ Success! Tracking Number: {tracking_num}")
            
            # Save the PDF Label
            label_data = response_dict.get('label_data') # Base64 encoded PDF
            if label_data:
                label_path = os.path.join(LABELS_DIR, f"{order_id}_{tracking_num}.pdf")
                with open(label_path, 'wb') as f:
                    # Canpar sometimes returns base64 string with newlines, remove them.
                    cleaned_label_data = label_data.replace('\n', '').replace('\r', '')
                    f.write(base64.b64decode(cleaned_label_data))
                print(f"   Saved PDF label to: {label_path}")
            
            # Save the XML Response (as JSON for readability)
            json_path = os.path.join(LABELS_DIR, f"{order_id}_response.json")
            with open(json_path, 'w') as f:
                json.dump(response_dict, f, indent=4)
            print(f"   Saved API response to: {json_path}")
            
            return {"status": "SUCCESS", "tracking_num": tracking_num, "error": "None"}
        else:
            # Check for error description in the response if success is not '1'
            # The 'error' element in the response is a dictionary with a 'description' field
            error_details = response_dict.get('error', {}).get('description', 'No specific error description provided.')
            print(f"❌ API Failed for Order {order_id}. Error: {error_details}")
            # If the response indicates failure, return the error details
            return {"status": "FAILED", "error": error_details, "response": response_dict}

    except Fault as f:
        print(f"❌ SOAP Fault occurred for Order {order_id}: {f}")
        return {"status": "SOAP_FAULT", "error": str(f)}
    except Exception as e:
        print(f"❌ An unexpected error occurred for Order {order_id}: {e}")
        return {"status": "EXCEPTION", "error": str(e)}

# ----------------------------------------------------------------------

def process_orders():
    """Main function to load orders, use Offer SKU, and call the API."""
    if not os.path.exists(INPUT_FILE):
        # NOTE: Removed file fetching logic as file is already uploaded, assuming this will be run locally by user.
        print(f"Input file not found: {INPUT_FILE}")
        return 1

    try:
        # Load orders data
        df = pd.read_csv(INPUT_FILE, encoding="utf-8-sig")
        # Ensure 'Quantity' and 'Total order amount incl. VAT' are numeric and handle NaN
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(1)
        df['Total order amount incl. VAT (including shipping charges)'] = pd.to_numeric(
            df['Total order amount incl. VAT (including shipping charges)'], errors='coerce'
        ).fillna(0.0)
        
        print(f"Loaded {len(df)} orders for processing.")

        # Initialize Canpar Client
        canpar_client = get_canpar_client()
        
        results = []
        for _, row in df.iterrows():
            order = row.to_dict()
            api_result = create_canpar_shipment(canpar_client, order)
            
            # Record-keeping
            order_record = {
                "Order number": order["Order number"],
                "Customer Name": f"{order['Shipping address first name']} {order['Shipping address last name']}",
                "SKU": order["Offer SKU"],
                "Quantity": order["Quantity"],
                "Tracking Number": api_result.get('tracking_num', 'N/A'),
                "API Status": api_result.get('status', api_result.get('response', {}).get('success', 'N/A')),
                "Error Details": api_result.get('error', 'None'),
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            results.append(order_record)
            
        print("\n" + "=" * 50)
        print("Processing Complete. Summary:")
        print("=" * 50)
        
        # Save final summary
        summary_df = pd.DataFrame(results)
        summary_file = os.path.join(OUTPUT_DIR, "Canpar_Shipment_Summary.xlsx")
        summary_df.to_excel(summary_file, index=False)
        print(f"Summary saved to: {summary_file}")

    except Exception as e:
        print(f"\nFATAL ERROR during processing: {str(e)}")
        return 1

if __name__ == "__main__":
    process_orders()