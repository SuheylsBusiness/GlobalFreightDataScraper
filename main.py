import requests
from bs4 import BeautifulSoup
import json
import os
import logging
import pandas as pd
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import configparser

def setup_logging():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('app.log', mode='a'),
                                  logging.StreamHandler()])
    logging.info("Logging setup complete.")

def read_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    logging.info("Configuration file read successfully.")
    return config

def read_json_file(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        logging.info(f"Successfully read JSON file: {file_path}")
        return data
    except Exception as e:
        logging.error(f"Error reading JSON file: {e}")
        return None

def save_to_json(data, file_path):
    try:
        with open(file_path, 'w') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        logging.info(f"Data successfully saved to JSON: {file_path}")
    except Exception as e:
        logging.error(f"Error saving to JSON: {e}")

def save_to_excel(data, file_path):
    try:
        df = pd.json_normalize(data)
        df.to_excel(file_path, index=False)
        logging.info(f"Data successfully saved to Excel: {file_path}")
    except Exception as e:
        logging.error(f"Error saving to Excel: {e}")

def get_company_urls(base_url, country_url, country_name):
    try:
        full_url = f"{base_url}{country_url}"
        response = requests.get(full_url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        companies = soup.select('div[class="company-name"] > a')
        logging.info(f"Company URLs retrieved for {country_name}.")
        return [(country_name, a['href']) for a in companies]
    except Exception as e:
        logging.error(f"Error getting company URLs for {country_name}: {e}")
        return []

def scrape_company_details(base_url, company_url):
    try:
        response = requests.get(f"{base_url}{company_url}", timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        details = {
            "CompanyName": soup.select_one('div[class="title-teaser-wrap"] > h1').text.strip(),
            "Address": '\n'.join([line.strip() for line in soup.select_one('div[class="address-wrap"]').text.split('\n')]).strip(),
            "OfferedServices": [li.text.strip() for li in soup.select('div[class="services"] > ul > li')],
            "CompanyId": soup.select_one('div[data-company]')['data-company'].strip()
        }
        logging.info(f"Details scraped for company at {company_url}.")
        return details
    except Exception as e:
        logging.error(f"Error scraping company details for URL {company_url}: {e}")
        return {}

def get_contact_details(base_url, company_id):
    try:
        response = requests.post(f"{base_url}/in.ajax_company_company",
                                 data=f"ajaxKey=&tx_wbdirectory_companies%5Baction%5D=showContact&tx_wbdirectory_companies%5Bcompany%5D={company_id}&tx_wbdirectory_companies%5Bcontroller%5D=Company",
                                 headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"},
                                 timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        return {
            "Phone": (soup.select_one('span[class="phone-icon"] > a')['href'].replace("tel:", "") if soup.select_one('span[class="phone-icon"] > a') else ""),
            "Email": (soup.select_one('span[class="mail-icon"] > a')['href'].replace("mailto:", "") if soup.select_one('span[class="mail-icon"] > a') else ""),
            "Website": (soup.select_one('span[class="site-icon trigger-click"] > a')['href'] if soup.select_one('span[class="site-icon trigger-click"] > a') else "")
        }
    except Exception as e:
        logging.error(f"Error getting contact details for company ID {company_id}: {e}")
        return {}

def connect_database(config):
    try:
        db_config = {
            "host": config["database"]["host"],
            "user": config["database"]["user"],
            "password": config["database"]["password"],
            "database": config["database"]["database"]
        }
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            logging.info("Successfully connected to the database.")
        return connection
    except Error as e:
        logging.error(f"Database connection failed: {e}")
        return None

def update_database(connection, records):
    if not records:
        logging.info("No records to update in the database.")
        return
    try:
        cursor = connection.cursor()
        for record in records:
            cursor.execute("SELECT CompanyId FROM your_table WHERE CompanyId = %s", (record['CompanyId'],))
            result = cursor.fetchone()
            if result:
                update_query = """
                    UPDATE your_table
                    SET CompanyName = %s, Address = %s, OfferedServices = %s, Phone = %s, Email = %s, Website = %s, Country = %s
                    WHERE CompanyId = %s
                """
                cursor.execute(update_query, (
                    record['CompanyName'],
                    record['Address'],
                    json.dumps(record['OfferedServices']),
                    record['Phone'],
                    record['Email'],
                    record['Website'],
                    record['Country'],
                    record['CompanyId'],
                ))
            else:
                insert_query = """
                    INSERT INTO your_table (CompanyId, CompanyName, Address, OfferedServices, Phone, Email, Website, Country)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (
                    record['CompanyId'],
                    record['CompanyName'],
                    record['Address'],
                    json.dumps(record['OfferedServices']),
                    record['Phone'],
                    record['Email'],
                    record['Website'],
                    record['Country'],
                ))
            connection.commit()
        logging.info("Database updated successfully.")
    except Error as e:
        logging.error(f"Database operation failed: {e}")
    finally:
        if connection.is_connected():
            cursor.close()

def main():
    setup_logging()
    config = read_config()
    debug_mode = config.getboolean('settings', 'Debug', fallback=False)

    base_url = config['application']['base_url']
    locations = read_json_file("locations.json")
    if locations is None:
        return
    allCompanyUrls = []

    counter = 0
    for location in locations:
        for country_name, details in location.items():
            allCompanyUrls += get_company_urls(base_url, details["Url"], country_name)
            counter += 1
            if debug_mode and counter == 5:
                break
        if debug_mode and counter == 5:
            break

    resultObjs = []
    for country_name, company_url in allCompanyUrls:
        company_details = scrape_company_details(base_url, company_url)
        if company_details:
            contact_details = get_contact_details(base_url, company_details["CompanyId"])
            resultObjs.append({**company_details, **contact_details, "Country": country_name})

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_folder = config['application']['output_folder']
    file_name = f"{output_folder}/{timestamp}_{len(resultObjs)}_records"
    os.makedirs(output_folder, exist_ok=True)
    save_to_json(resultObjs, f"{file_name}.json")
    save_to_excel(resultObjs, f"{file_name}.xlsx")

    db_connection = connect_database(config)
    if db_connection:
        update_database(db_connection, resultObjs)
        db_connection.close()

    logging.info("Script execution completed.")

if __name__ == "__main__":
    main()