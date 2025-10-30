import re
import schedule
import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from datetime import datetime
import os

def extract_data(driver, url):
    driver.get(url)
    driver.implicitly_wait(60)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    
    links = [data.a['href'] for data in soup.find_all("div", class_='box-content')]
    dates = [convert_to_yyyy_mm_dd(data.time['datetime']) for data in soup.find_all("div", class_='gmr-meta-topic')]
    
    return links, dates

def extract_positions_locations_company(links, driver,dates):
    positions, locations, Company, link_fix, dates_fix = [], [], [], [], []
    i = 0
    for link in links:
        driver.get(link)
        driver.implicitly_wait(60)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        positions_div = soup.find("div", class_="entry-content entry-content-single clearfix")
        if positions_div:
            pos_list = [pos.get_text(strip=True) for pos in positions_div.find_all('h3')]  
        else:
            pos_list = ['-']
        positions.extend(pos_list)
        
        comp = soup.find("header", class_="entry-header entry-header-single")
        comp_text = comp.find('h1').get_text(strip=True) if comp and comp.find('h1') else '-'
        Company.extend([comp_text] * len(pos_list)) 
        
        loc = soup.find("span", class_="0-cl")
        loc_text = loc.find('a').get_text(strip=True) if loc and loc.find('a') else '-'
        locations.extend([loc_text] * len(pos_list))  
        
        link_fix.extend([link] * len(pos_list))
        dates_fix.extend([dates[i]] * len(pos_list))
        i += 1
    return positions, locations, Company, link_fix, dates_fix

def clean_positions(position):
    return re.sub(r'^\d+\.\s*', '', position)

def convert_to_yyyy_mm_dd(date_str):
    return datetime.fromisoformat(date_str).strftime('%Y-%m-%d')

def scrape_and_save():
    start_time = time.time()
    
    driver = webdriver.Chrome(service=ChromeService(executable_path=r"C:\Users\awopro06w000\Documents\Astra World\chromedriver-win32\chromedriver.exe"))
    url = 'https://karirpurwokerto.id/'
    
    links, dates = extract_data(driver, url)
    if len(links) != len(dates):
        dates = dates[len(dates) - len(links):]
    positions, locations, Company, link_fix, dates_fix = extract_positions_locations_company(links, driver,dates)
    
    if len(links) != len(dates):
        dates = dates[len(dates) - len(links):]
    data = pd.DataFrame({'Link': link_fix, 'Tanggal': dates_fix, 'Perusahaan': Company, 'Posisi': positions, 'Lokasi': locations})
    data['Posisi'] = data['Posisi'].apply(clean_positions)
    data['Perusahaan'] = data['Perusahaan'].astype(str)
    data['Perusahaan'] = data['Perusahaan'].str.replace('^Lowongan Kerja ', '', regex=True)
    data['Lokasi'] = data['Lokasi'].replace(['Pilihan'], '-')

    filename = f'C:/Users/awopro06w000/Downloads/Scrap_Purwokerto_{datetime.now().strftime("%Y-%m-%d")}.xlsx'
    
    if os.path.exists(filename):
        existing_data = pd.read_excel(filename)
        data = pd.concat([existing_data, data]).drop_duplicates(subset=['Link','Posisi'], keep='last')
    
    data.to_excel(filename, index=False)
    print(f"Data saved to {filename}")
    
    driver.quit()
    print(f"Durasi {time.time() - start_time:.2f} detik.")


def job():
    scrape_and_save()
    
schedule.every().day.at("09:00").do(job)
schedule.every().day.at("14:00").do(job)
schedule.every().day.at("17:00").do(job)
schedule.every().day.at("23:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(60)
