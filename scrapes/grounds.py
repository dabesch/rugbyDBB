""" grounds.py
ETL for the stadium ground page.

"""
import requests
from bs4 import BeautifulSoup
from database import executeQuery, createSQL


def scrape(groundID):
    """

    :param groundID:
    :return:
    """
    url = f'http://en.espn.co.uk/scrum/rugby/ground/{groundID}.html'
    response = requests.get(url).text
    #response = open('../html/twik.html')
    soup = BeautifulSoup(response, 'lxml')

    groundDict = {'groundid': groundID}
    header = soup.find('caption', {'class': 'ScrumSectionHeader'})
    groundDict['groundName'] = header.find('div', {'class': 'scrumPlayerName'}).text.strip()
    groundDict['groundLocation'] = header.find('div', {'class': 'scrumPlayerCountry'}).text.strip()

    desc = soup.find_all('div', {'class': 'scrumPlayerDesc'})
    groundDict.update(descStrip(desc))

    sql = createSQL(groundDict, 'grounds')
    executeQuery(sql)


def descStrip(desc):
    """

    :param desc:
    :return:
    """
    descDict = {}
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
        elif label =='Home team(s)':
            label = 'Hometeam'
        value = d.text.replace(label, '').strip()
        descDict[label.replace(' ', '')] = value
        i += 1
    # Clean values
    if descDict['address']:
        descDict['address'] = descDict['address'].replace('\n', ', ')
        descDict['postcode'] = descDict['address'].split(',')[-1].strip()
    if descDict['Capacity']:
        descDict['Capacity'] = descDict['Capacity'].replace(',', '')
    descDict.pop('Time', None)
    return descDict

scrape('16145')
