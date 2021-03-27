""" playersStats.py
ETL for the player stats from their relative match stats page.
Scrapes the web page for a player and writes it to the players table in the database.
Each player has a entry for each match they played in.
"""
import requests
from bs4 import BeautifulSoup
from pandas import to_datetime

from database import execute_query, create_SQL


def scrape(player_id):
    """
    Collects the player stats for all matched and inputs the results to the database
    :param player_id: The Id of the player to collect summary data from
    :return: Executes the results and sends the results to the database
    """
    url = f'http://en.espn.co.uk/statsguru/rugby/player/{player_id}.html?class=1;template=results;type=player;view=match'
    response = requests.get(url).text
    soup = BeautifulSoup(response, 'lxml')

    tb = soup.find_all('table', {'class': 'engineTable'})[2]
    rows = tb.find_all('tr', {'class': 'data1'})

    # Process each row in the table
    for r in rows:
        results = process_row(r, player_id)
        sql = create_SQL(results, 'playerStats')
        execute_query(sql)


def process_row(row, player_id):
    """
    Takes a row from the player stats table, handles the "hidden" empty column by only including columns with values in
    the values object
    :param row: a row for a players performance in a match
    :param player_id: the playerID of the player involved, this is only to create the dictionary entry
    :return: a dictionary with all of the fields of the table
    """
    labels = ['pos', 'pts', 'tries', 'conv', 'pens', 'dropG', 'result', 'team', 'opposition', 'ground', 'matchDate',
              'matchlink']
    values = [i.text for i in row.find_all('td') if i.text != '']
    hrefs = [i['href'] for i in row.find_all('a', href=True)]

    matchLink, matchID, groundID = href_category(hrefs)

    # Create dict
    match_dict = dict(matchlink=matchLink,
                      matchID=matchID,
                      groundID=groundID,
                      playerID=player_id,
                      )
    for lab, val in zip(labels, values):
        match_dict[lab] = val

    # Clean fields in dict
    match_dict['matchDate'] = str(to_datetime(match_dict['matchDate']))
    match_dict['opposition'] = match_dict['opposition'].strip('v ')

    if '(' in match_dict['pos']:
        match_dict['pos'] = match_dict['pos'].strip('()')
        match_dict['startGame'] = 0
    else:
        match_dict['startGame'] = 1

    return match_dict


def href_category(hrefs):
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
