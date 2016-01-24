#!/usr/bin/python

# standard libs
import sqlite3
import json
import time
import logging
import sys
from random import random

# third-party libs
import requests
from bs4 import BeautifulSoup

logging.basicConfig(filename='scraper.log',
                    level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',)
con = sqlite3.connect('re.db')
cur = con.cursor()

cur.execute('''
                CREATE TABLE IF NOT EXISTS listings
                    (postid text, subject text, price int, href text, posttime int,
                     beds int, size int, lat real, lon real, accuracy real,
                     description text, attributes text, dbupdatetime int)
             ''')


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


def parse_listing(this_list):
    '''Parse initial search listing for housing data'''
    this_id = this_list.find('span', attrs={'class': 'pl'}).find('a')['data-id']
    this_subject = this_list.find('a', attrs={'class': 'hdrlnk'}).text
    this_price = this_list.find('span', attrs={'class': 'price'})
    if this_price is None:
        this_price = None
    else:
        this_price = this_price.findAll(text=True)[0].strip('$')
    this_href = this_list.find('a', attrs={'class': 'hdrlnk'}).get('href')
    this_time = int(time.mktime(time.strptime(this_list.find('time')['datetime'], '%Y-%m-%d %H:%M')))
    housing = this_list.find('span', attrs={'class': 'housing'})
    if housing is None:
        housing = 'br'  # TODO cheap workaround I'll fix later
    else:
        housing = housing.text
    this_beds, this_size = get_beds_and_size(housing)
    this_listing = [this_id, this_subject, this_price, this_href, this_time,
                    this_beds, this_size]
    return this_listing


def parse_post(post):
    this_id = post.find_all('p', {'class': 'postinginfo'})[1].text.strip('post id : ')
    this_loc = post.find('div', {'id': 'map', 'class': 'viewposting'})
    if this_loc is None:
        this_lat = None
        this_lon = None
        this_acc = None
    else:
        this_lat = this_loc['data-latitude']
        this_lon = this_loc['data-longitude']
        this_acc = this_loc['data-accuracy']
    this_desc = post.find('section', {'id': 'postingbody'}).text.strip('\n')
    attrgroup = post.find_all('p', {'class': 'attrgroup'})

    attrs = []
    for attr in attrgroup:
        att = attr.find_all('span')
        for a in att:
            attrs.append(a.text)
    attrs = json.dumps(attrs)

    this_post = [this_lat, this_lon, this_acc, this_desc, attrs, this_id]
    return this_post


def process_listings(listing):
    epochms_time = int(time.time() * 1000)
    listing.append(epochms_time)
    con.execute('INSERT INTO listings (postid, subject, price, href, posttime, beds, size, dbupdatetime) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', listing)

base = 'http://portland.craigslist.org'
srch = '/search/apa'
resp = requests.get(base + srch)
html = BeautifulSoup(resp.text, 'html.parser')
current_listings = html.find_all('p', attrs={'class': 'row'})

last_post_time = cur.execute('SELECT max(posttime) FROM listings').fetchone()
if last_post_time[0] == None:
    last_post_time = (0,)
else:
    pass

for listing in current_listings:
    listing = parse_listing(listing)
    if int(last_post_time[0]) < listing[4]:
        process_listings(listing)
        logging.info('Processing listing, %s', listing[0])
    else:
        pass

con.commit()
logging.info('Listings processed and commit complete.')

posts = cur.execute('SELECT postid, href FROM listings WHERE posttime > ?', last_post_time).fetchall()

for post in posts:
    rnd = random()
    time.sleep(rnd)
    r = requests.get(base + post[1])
    post_body = BeautifulSoup(r.text, 'html.parser')
    lat, lon, accuracy, description, attributes, postid = parse_post(post_body)
    logging.info('Processing post %s after waiting %s seconds', post[0], rnd)
    cur.execute('UPDATE listings SET lat = ?, lon = ?, accuracy = ?, description = ?, attributes = ? WHERE postid = ?', (lat, lon, accuracy, description, attributes, postid))

con.commit()
logging.info('%s postings processed and commit complete.', len(posts))
con.close()
