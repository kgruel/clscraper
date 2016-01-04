# standard libs
import sqlite3
import time

# third-party libs
import requests
from bs4 import BeautifulSoup

con = sqlite3.connect('re.db')
cur = con.cursor()

cur.execute('DROP TABLE IF EXISTS listings')

cur.execute('''CREATE TABLE IF NOT EXISTS listings
             (id int, subject text, price int, href text, time text, beds int, size int)''')

base = 'http://portland.craigslist.org'
srch = '/search/apa'
resp = requests.get(base + srch)
html = BeautifulSoup(resp.text, 'html.parser')
listings = html.find_all('p', attrs={'class': 'row'})

def get_beds_and_size(housing):
    split = housing.strip('/- ').split(' - ')
    if len(split) == 2:
        beds = split[0].replace('br', '')
        size = split[1].replace('ft2', '')
    elif 'br' in split[0]:
        beds = split[0].replace('br', '')
        size = None
    elif 'ft2' in split[0]:
        beds = None
        size = split[0].replace('ft2', '')
    return beds, size

def process_listings(this_list):
    '''Parse initial search listing for housing data'''
    this_id = this_list.find('span', attrs={'class': 'pl'}).find('a')['data-id']
    this_subject = this_list.find('a', attrs={'class': 'hdrlnk'}).text
    this_price = this_list.find('span', attrs={'class': 'price'}).findAll(text = True)[0].strip('$')
    this_href = this_list.find('a', attrs={'class': 'hdrlnk'}).get('href')
    this_time = this_list.find('time')['datetime']
    housing = this_list.find('span', attrs={'class': 'housing'})
    if housing is None:
        housing = 'br'
    else:
        housing = housing.text
    this_beds, this_size = get_beds_and_size(housing)
    this_listing = [this_id, this_subject, this_price, this_href, this_time, this_beds, this_size]
    return this_listing

for this_list in listings:
    this_listing = process_listings(this_list)
    con.execute('INSERT INTO listings VALUES (?, ?, ?, ?, ?, ?, ?)', this_listing)

con.commit()

for row in con.execute('SELECT href FROM listings'):
    print(base + row[0])

con.close()
