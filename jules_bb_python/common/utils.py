import sys
import os

# Adjust the path to import from the project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def get_best_buy_api_key():
    """
    Retrieves the Best Buy API key from the config file.
    Returns the API key or None if not found.
    """
    try:
        from app_config import BEST_BUY_API_KEY
        return BEST_BUY_API_KEY
    except (ImportError, ModuleNotFoundError):
        print("ERROR: Could not import BEST_BUY_API_KEY from app_config.py.")
        print("Please ensure jules_bb_python/app_config.py exists and contains the BEST_BUY_API_KEY.")
        return None

def get_canpar_credentials():
    """
    Retrieves Canpar API credentials and shipper info from the config file.
    """
    try:
        from app_config import CANPAR_API_USER, CANPAR_API_PASSWORD, CANPAR_SHIPPER_NUM, PICKUP_ADDRESS
        return {
            "user": CANPAR_API_USER,
            "password": CANPAR_API_PASSWORD,
            "shipper_num": CANPAR_SHIPPER_NUM,
            "pickup_address": PICKUP_ADDRESS
        }
    except (ImportError, ModuleNotFoundError) as e:
        print(f"ERROR: Could not import Canpar credentials from app_config.py: {e}")
        return None