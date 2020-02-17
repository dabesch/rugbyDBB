""" players.py
ETL for the player summary page.
Scrapes the web page for a player and writes it to the playerStats table in the database.
Cleans the data throughout the process. Produces a field called relationsJSON which is a field for a later ETL process.
"""
import requests
from bs4 import BeautifulSoup
from pandas import to_datetime
from database import executeQuery, createSQL


def scrape(playerID):
    """
    Collects the player summary and inputs the results to the database
    :param playerID: The Id of the player to collect summary data from
    :return: Executes the results and sends the results to the database
    """
    url = f'http://en.espn.co.uk/statsguru/rugby/player/{playerID}.html'
    response = requests.get(url).text
    soup = BeautifulSoup(response, 'lxml')

    desc = soup.find_all('div', class_="scrumPlayerDesc")

    playerDict = descStrip(desc)

    caption = soup.find('caption', {'class': 'ScrumSectionHeader'})
    name = caption.find('div', {'class': 'scrumPlayerName'}).text
    nation = caption.find('div', {'class': 'scrumPlayerCountry'}).text
    playerDict['name'] = name.strip()
    playerDict['nation'] = nation.strip()
    playerDict['playerID'] = playerID

    sql = createSQL(playerDict, 'players')
    executeQuery(sql)


def descStrip(desc):
    """
    Separates out each of the titles and values form the player description and produces the results as a dictionary
    :param desc: A description object which contains the results 'scrumPlayerDesc' class
    :return: a dictionary object of the processed 'scrumPlayerDesc' class
    """
    playerDict = {}
    empty = 0
    for d in desc:
        tup = d.contents
        try:
            label = tup[0].text.strip().replace(' ', '')
        except:
            empty += 1
            label = 'empty'
        if len(tup) == 2:
            value = tup[1].strip()
            playerDict[label] = value
        elif label == 'Relations':

            value, valueJSON = getRelations(d)
            playerDict[label] = value
            playerDict['RelationsJSON'] = valueJSON
    print(f'{empty} empty labels')
    return cleanDesc(playerDict)


def getRelations(relations):
    """
    Takes the object containing multiple spans and returns cleaned results
    :param relations: The relations fields of the summary page
    :return: A tuple of a string object with the relations in and a dictionary as a string object which can be used
             later for creating a relations table.
    """
    json = {}
    relList = []
    for r in relations.find_all('span'):
        json[r.text] = r.a['href'].split('/')[-1].split('.')[0]
        relList.append(r.text)
    json = str(json).replace("'", '"')

    return ' '.join(relList), json


def cleanDesc(playerDict):
    """
    Cleans certain fields so that they will fit with the database. Also splits some fields to create new fields such as
    names and hometown.
    :param playerDict: the playerDict dictionary object which needs cleaning
    :return: the processed and cleaned dictionary object
    """
    if 'Born' in playerDict:
        dob = playerDict['Born'].split(',')[:2]
        home = playerDict['Born'].split(',')[2:]
        playerDict['Born'] = str(to_datetime(''.join(dob)))
        playerDict['Hometown'] = ''.join(home).strip()

    if 'Died' in playerDict:
        died = playerDict['Died'].split(',')[:2]
        place = playerDict['Born'].split(',')[2:]
        playerDict['Died'] = str(to_datetime(''.join(died)))
        playerDict['placeOfDeath'] = ''.join(place).strip()

    playerDict.pop('Currentage', None)
    playerDict['Firstname'] = playerDict['Fullname'].split(' ')[0]
    playerDict['Lastname'] = playerDict['Fullname'].split(' ')[-1]

    for meas in ['Height', 'Weight']:
        playerDict[meas] = convertUnits(playerDict[meas], meas)

    return playerDict


def convertUnits(measure, category):
    """
    ESPN stores measurements in imperial units, this function converts to metric
    :param measure: the measurement string, either
    :param category: weight/height
    :return: returns the cleaned measurement
    """
    if category.lower() == 'weight':
        # pounds to kg
        weight = int(measure.strip('lb'))
        weight = weight * 0.453592
        return round(weight, 2)
    if category.lower() == 'height':
        # feet and inches to cm
        f, i = [m for m in measure.split(' ') if m.isdigit()]
        height = (int(f) * 30.48) + int(i) * 2.54
        return round(height, 2)

# todo: later processing stage for relations key, data stored in JSON format currently
