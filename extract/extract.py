import os
import time
import glob
import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# ================== CONFIGURATION ==================
DOWNLOAD_DIR = r"C:\HF_Data_Downloads"     # Folder where files will be saved
FORMAT = "CSV"                             # "CSV" or "Parquet"
TIMEOUT = 30                               # Max wait time for page elements & downloads
HEADLESS = False                           # Set to True to run without visible browser
DELAY_BETWEEN_TICKERS = 3                  # Seconds to wait between downloads (be gentle)
# ===================================================

def get_sp500_tickers():
    """Scrape the list of S&P 500 tickers from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        tables = pd.read_html(url)
        # The first table contains the constituents
        df = tables[0]
        tickers = df['Symbol'].tolist()
        # Clean tickers: remove any dots (e.g., BRK.B -> BRK.B is fine, but some have '.' which might need adjustment)
        # HF Data Library likely expects standard tickers like BRK.B, so we keep as is.
        return tickers
    except Exception as e:
        print(f"Error fetching S&P 500 list: {e}")
        # Fallback to a small sample in case of failure
        return ["SPY", "AAPL", "MSFT", "AMZN", "GOOGL"]

def setup_driver():
    """Configure and return Chrome WebDriver with download preferences."""
    chrome_options = Options()
    if HEADLESS:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    
    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def wait_for_download(initial_files, ticker, timeout=TIMEOUT):
    """Wait until a new .csv or .parquet file appears, then rename it with ticker."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        current_files = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*")))
        new_files = current_files - initial_files
        for file in new_files:
            if file.endswith((".csv", ".parquet")):
                ext = os.path.splitext(file)[1]
                new_name = os.path.join(DOWNLOAD_DIR, f"{ticker}_{int(time.time())}{ext}")
                os.rename(file, new_name)
                print(f"Downloaded: {new_name}")
                return True
        time.sleep(1)
    print(f"Timeout waiting for {ticker} download.")
    return False

def download_ticker_data(driver, ticker):
    """Search for ticker, select it, and click the desired format button."""
    try:
        # Locate search input
        search_input = WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Search for a ticker']"))
        )
        search_input.clear()
        search_input.send_keys(ticker)
        time.sleep(1.5)  # Allow suggestion dropdown to populate
        
        # Click on the dropdown suggestion matching the ticker
        # Note: The exact class may vary – adjust if needed
        suggestion = WebDriverWait(driver, TIMEOUT).until(
            EC.element_to_be_clickable((By.XPATH, f"//div[contains(@class, 'suggestion') and contains(text(), '{ticker}')]"))
        )
        suggestion.click()
        
        # Wait for format buttons and click the desired one
        format_button = WebDriverWait(driver, TIMEOUT).until(
            EC.element_to_be_clickable((By.XPATH, f"//button[contains(text(), '{FORMAT}')]"))
        )
        
        initial_files = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*")))
        format_button.click()
        print(f"Clicked {FORMAT} for {ticker}")
        
        if wait_for_download(initial_files, ticker):
            print(f"Successfully downloaded {ticker}")
        else:
            print(f"Failed to download {ticker}")
            
    except Exception as e:
        print(f"Error processing {ticker}: {e}")

def main():
    # Create download folder if it doesn't exist
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    # Get S&P 500 tickers
    tickers = get_sp500_tickers()
    print(f"Retrieved {len(tickers)} S&P 500 tickers. First 5: {tickers[:5]}")
    
    driver = setup_driver()
    url = "https://hfdatalibrary.com/pages/download"
    driver.get(url)
    
    # Accept cookies if a consent popup appears
    try:
        cookie_btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accept')]"))
        )
        cookie_btn.click()
    except:
        pass
    
    # Loop through all S&P 500 tickers
    for idx, ticker in enumerate(tickers, start=1):
        print(f"Processing {idx}/{len(tickers)}: {ticker}")
        download_ticker_data(driver, ticker)
        time.sleep(DELAY_BETWEEN_TICKERS)
    
    driver.quit()
    print("All S&P 500 downloads completed.")

if __name__ == "__main__":
    main()