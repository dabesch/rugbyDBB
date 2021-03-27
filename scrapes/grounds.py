""" grounds.py
ETL for the stadium ground page.
Scrapes the web page for a ground/stadium and writes it to the grounds table in the database.
Cleans the data throughout the process. ESPN's grounds database is a variety of different qualities so most fields may
be NULLs
"""
import requests
from bs4 import BeautifulSoup
from database import executeQuery, createSQL


def scrape(groundID):
    """
    Collects the ground information and inputs to the database
    :param playerID: The Id of the player to collect summary data from
    :return: Executes the results and sends the results to the database
    """
    url = f'http://en.espn.co.uk/scrum/rugby/ground/{groundID}.html'
    response = requests.get(url).text
    soup = BeautifulSoup(response, 'lxml')

    groundDict = {'groundid': groundID}
    header = soup.find('caption', {'class': 'ScrumSectionHeader'})
    groundDict['groundName'] = header.find('div', {'class': 'scrumPlayerName'}).text.strip()
    groundDict['groundLocation'] = header.find('div', {'class': 'scrumPlayerCountry'}).text.strip()

    desc = soup.find_all('div', {'class': 'scrumPlayerDesc'})
    groundDict.update(strip_description(desc))

    sql = createSQL(groundDict, 'grounds')
    executeQuery(sql)


def strip_description(desc):
    """
    Separates out each of the titles and values form the player description and produces the results as a dictionary
    :param desc: A description object which contains the results 'scrumPlayerDesc' class
    :return: a dictionary object of the processed 'scrumPlayerDesc' class
    """
    desc_dict = {}
    un = 0
    i = 0
    for d in desc:
        if d.b:
            label = d.b.text
        elif i == 0:
            label = 'address'
        else:
            label = f'unknown{un}'
            un += 1
        if label == 'Also or formerly known as':
            label = 'otherNames'
        elif label == 'Home team(s)':
            label = 'Hometeam'
        value = d.text.replace(label, '').strip()
        desc_dict[label.replace(' ', '')] = value
        i += 1
    # Clean values
    if desc_dict['address']:
        desc_dict['address'] = desc_dict['address'].replace('\n', ', ')
        desc_dict['postcode'] = desc_dict['address'].split(',')[-1].strip()
    if desc_dict['Capacity']:
        desc_dict['Capacity'] = desc_dict['Capacity'].replace(',', '')
    desc_dict.pop('Time', None)
    return desc_dict
