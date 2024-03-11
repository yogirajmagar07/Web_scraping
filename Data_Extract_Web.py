from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine
def Get_Row_Data(url):
    html_data = requests.get(url)
    soup = BeautifulSoup(html_data.content, 'lxml')
    return soup

def Data_Extract(soup):
    stores_data = []
    for card in soup.find_all('div', class_='card mb-2'):
        store_name = card.h2.a.text.strip()
        phone_element = card.find('i', class_='fa-phone')
        phone = phone_element.next_sibling.strip() if phone_element else None
        address_lines = card.find_all('div', class_='col-12 col-md-4')[1].stripped_strings
        address = ', '.join(address_lines).strip()
        map_url = soup.find('div', class_='col-12 col-md-4 d-none d-md-block').a['href']
        match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', map_url)
        latitude = match.group(1) if match else None
        longitude = match.group(2) if match else None
        city_and_postal_code = re.search(r'QLD (\d+)', address)
        postal_code = city_and_postal_code.group(1) if city_and_postal_code else None
        store_data = {
            'Brand_Name': store_name,
            'Phone': phone,
            'city': store_name,  
            'Latitude': latitude,
            'Longitude': longitude,
            'Store_Address': address,
            'Postal_Code': postal_code, 
            'Country': 'AU'
        }
        stores_data.append(store_data)

    return stores_data

def Process_Data(stores_data):
    data = pd.DataFrame(stores_data)
    data[['Address','City','postal_code','country','Schedule']] = data['Store_Address'].str.split(',', 4, expand=True)
    data['Address'] = data['Address'] + ', ' + data['City']
    data = data.drop(columns=['Schedule','City','postal_code','country','Schedule'])
    return data

def Store_data_SQL(data):
    db_user = 'root'
    db_password = 'Abhi07mh45'
    db_host = 'localhost'
    db_name = 'Company_data'
    engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")
    data.to_sql('store_info', con=engine, if_exists='replace', index=False)

    data_details = pd.DataFrame({
        'Brand_Name': ['Ashmore', 'Bethania', 'Browns Plains', 'Bundaberg', 'Burleigh','Chancellor Park', 'Deception Bay', 'Gladstone', 'Helensvale','Jindalee', 'Kallangur', 'Kawana Waters', 'Kingaroy', 'Lismore','Loganholme', 'Maryborough', 'Mermaid Waters', 'Morayfield','Noosaville', 'Pialba (Hervey Bay)', 'Redbank', 'Richlands','Rothwell', 'Southport', 'Townsville (Kirwan)','Tweed Heads South', 'Underwood', 'Upper Coomera', 'Windsor'],
        'Open_Hours': ['Mon-Sun: 8:30 AM to 5:00 PM']*29
    })

    data_details.to_sql('store_details', con=engine, if_exists='replace', index=False)
    engine.dispose()
    print("Data has been successfully saved to MySQL database.")
    
    return data_details,data

def Convert_csv(data_details,data):
    merged_data = pd.merge(data, data_details, on='Brand_Name')
    merged_data.to_csv('C:\\Users\\YOGIRAJ\\Downloads\\ brand_store_info7.csv')
    return "Successfully Uploaded data into csv file..."

if __name__=="__main__":
    url ="https://www.choicethediscountstore.com.au/store-finder/"
    soup=Get_Row_Data(url)
    stores_data=Data_Extract(soup)
    data=Process_Data(stores_data)
    data_details,data=Store_data_SQL(data)
    final_data=Convert_csv(data_details,data)
    print(final_data)
