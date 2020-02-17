""" playersStats.py
ETL for the player stats from their relative match stats page.
Scrapes the web page for a player and writes it to the players table in the database.
Each player has a entry for each match they played in.
"""
import requests
from bs4 import BeautifulSoup
from pandas import to_datetime
from database import executeQuery, createSQL


def scrape(playerID):
    """
    Collects the player stats for all matched and inputs the results to the database
    :param playerID: The Id of the player to collect summary data from
    :return: Executes the results and sends the results to the database
    """
    url = f'http://en.espn.co.uk/statsguru/rugby/player/{playerID}.html?class=1;template=results;type=player;view=match'
    response = requests.get(url).text
    soup = BeautifulSoup(response, 'lxml')

    tb = soup.find_all('table', {'class': 'engineTable'})[2]
    rows = tb.find_all('tr', {'class': 'data1'})

    # Process each row in the table
    for r in rows:
        results = rowProcess(r, playerID)
        sql = createSQL(results, 'playerStats')
        executeQuery(sql)


def rowProcess(row, playerID):
    """
    Takes a row from the player stats table, handles the "hidden" empty column by only including columns with values in
    the values object
    :param row: a row for a players performance in a match
    :param playerID: the playerID of the player involved, this is only to create the dictionary entry
    :return: a dictionary with all of the fields of the table
    """
    labels = ['pos', 'pts', 'tries', 'conv', 'pens', 'dropG', 'result', 'team', 'opposition', 'ground', 'matchDate',
              'matchlink']
    values = [i.text for i in row.find_all('td') if i.text != '']
    hrefs = [i['href'] for i in row.find_all('a', href=True)]

    matchLink, matchID, groundID = hrefCat(hrefs)

    # Create dict
    matchDict = dict(matchlink=matchLink,
                     matchID=matchID,
                     groundID=groundID,
                     playerID=playerID,
                     )
    for l, v in zip(labels, values):
        matchDict[l] = v

    # Clean fields in dict
    matchDict['matchDate'] = str(to_datetime(matchDict['matchDate']))
    matchDict['opposition'] = matchDict['opposition'].strip('v ')

    if '(' in matchDict['pos']:
        matchDict['pos'] = matchDict['pos'].strip('()')
        matchDict['startGame'] = 0
    else:
        matchDict['startGame'] = 1

    return matchDict


def hrefCat(hrefs):
    """
    Process the href object and extracts out id's for later use, specifically match and ground id's
    :param hrefs: a soup object which will contain href links to other parts of the site - which contain id values
    :return: matchLink, matchID and groundID values
    """
    matchLink = None
    matchID = None
    groundID = None
    for h in hrefs:
        if 'match' in h:
            matchLink = h
            matchID = h.split('/')[-1].split('.')[0]
        elif 'ground' in h:
            groundID = h.split('/')[-1].split('.')[0]

    return matchLink, matchID, groundID
