
# Standard library imports
import os
import time

# Third-party imports
import requests
import dotenv
import yaml
import pandas as pd

# Local imports
import marketstack
from mac_notifications import client


# =========================
# Configuration and Setup
# =========================

def load_config():
    with open("config.yaml", "r") as file:
        return yaml.safe_load(file)

def setup_environment():
    dotenv.load_dotenv()
    api_key = os.getenv("METAL_PRICE_API")
    marketstack.set_api_key(os.getenv("MARKETSTACK_API"))
    return api_key

# =========================
# Utility Functions
# =========================

def get_etf_price(etf_symbol):
    try:
        etf_data = marketstack.EndOfDay(etf_symbol).get_data_df()
        etf_data.sort_values(by='date', ascending=False, inplace=True)
        
        if not etf_data.empty:
            return etf_data, etf_data['close'].values[0]
        else:
            print(f"No data found for ETF: {etf_symbol}")
            return None
    except Exception as e:
        print(f"Exception while fetching ETF data for {etf_symbol}: {e}")
        return None

def check_etf_price(etf_symbol, threshold, alert_enabled):
    df, price = get_etf_price(etf_symbol)
    if price is not None:
        print(f"{etf_symbol} current price: ₹{price:.2f}")
        if alert_enabled and price >= threshold:
            send_mac_notification(f"🚨 {etf_symbol} Alert", f"{etf_symbol} has reached ₹{price:.2f} 🚨")
    else:
        print(f"Failed to retrieve price for {etf_symbol}")

    return df

def get_usd_to_inr(api_key):
    url = f"https://api.metalpriceapi.com/v1/convert?api_key={api_key}&from=USD&to=INR&amount=1"
    try:
        response = requests.get(url)
        data = response.json()
        if response.status_code == 200 and "result" in data:
            return data["result"]
        else:
            print(f"API error: {data}")
            return None
    except Exception as e:
        print(f"Exception during API request: {e}")
        return None

def send_mac_notification(title, message):
    client.create_notification(
        title=title,
        subtitle=message
    )

# =========================
# Main Execution
# =========================

def main():
    config = load_config()
    api_key = setup_environment()

    # Load configuration values
    SIVR_THRESHOLD = config['SIVR_threshold']
    GLDM_THRESHOLD = config['GLDM_threshold']
    USD_TO_INR_THRESHOLD = config['USD_to_INR_threshold']
    USD_TO_INR_ALERT = config['USD_to_INR_alert']
    SIVR_ALERT = config['SIVR_alert']
    GLDM_ALERT = config['GLDM_alert']

    # Load data from parquet files if available
    if os.path.exists("Gold prices.parquet"):   
        gold_df = pd.read_parquet("Gold prices.parquet")
    elif os.path.exists("Silver prices.parquet"): 
        silver_df = pd.read_parquet("Silver prices.parquet")
    elif os.path.exists("USD to INR.parquet"):
        usd_inr_df = pd.read_parquet("USD to INR.parquet")
        print("Data loaded from parquet files.")
    else:
        gold_df = pd.DataFrame()
        silver_df = pd.DataFrame()
        usd_inr_df = pd.DataFrame() 

    # Check Silver ETF price
    if SIVR_ALERT:
        df = check_etf_price('SIVR', SIVR_THRESHOLD, SIVR_ALERT)

    # Check Gold ETF price
    if GLDM_ALERT:
        df = check_etf_price('GLDM', GLDM_THRESHOLD, GLDM_ALERT)

    # Check USD to INR conversion rate
    if USD_TO_INR_ALERT:
        rate = get_usd_to_inr(api_key)
        if rate:
            print(f"1 USD = ₹{rate:.2f}")
            if rate >= USD_TO_INR_THRESHOLD:
                send_mac_notification("💰 USD to INR Alert", f"1 USD = ₹{rate:.2f} 🚨")
        else:
            print("Failed to retrieve conversion rate.")

    # Save data to parquet files
    gold_df.to_parquet("Gold prices.parquet")
    silver_df.to_parquet("Silver prices.parquet")
    usd_inr_df.to_parquet("USD to INR.parquet")


if __name__ == "__main__":
    main()