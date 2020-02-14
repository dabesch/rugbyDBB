import requests
from bs4 import BeautifulSoup
from datetime import datetime
from database import executeQuery, createSQL


def scrape(playerID):
    url = f'http://en.espn.co.uk/statsguru/rugby/player/{playerID}.html?class=1;template=results;type=player;view=match'
    response = requests.get(url).text
    print(f'Connection made player:{playerID}:{datetime.now()}')
    #response = open('html/player.html')
    soup = BeautifulSoup(response, 'lxml')

    tb = soup.find_all('table', {'class': 'engineTable'})[2]
    rows = tb.find_all('tr', {'class': 'data1'})

    for r in rows:
        results = rowAppend(r, playerID)
        sql = createSQL(results, 'playerStats')
        executeQuery(sql)


def rowAppend(row, playerID):
    labels = ['pos', 'pts', 'tries', 'conv', 'pens', 'dropG', 'result', 'team', 'opposition', 'ground', 'matchDate',
              'matchlink']
    values = [i.text for i in row.find_all('td') if i.text != '']
    hrefs = [i['href'] for i in row.find_all('a', href=True)]

    matchLink, matchID, groundID = hrefCat(hrefs)

    matchDict = dict()
    for l, v in zip(labels, values):
        matchDict[l] = v
    matchDict['matchlink'] = matchLink
    matchDict['matchID'] = matchID
    matchDict['groundID'] = groundID
    matchDict['playerID'] = playerID
    matchDict['matchDate'] = str(datetime.strptime(matchDict['matchDate'], '%d %b %Y'))
    matchDict['opposition'] = matchDict['opposition'].strip('v ')

    if '(' in matchDict['pos']:
        matchDict['pos'] = matchDict['pos'].strip('()')
        matchDict['startGame'] = 0
    else:
        matchDict['startGame'] = 1

    return matchDict


def hrefCat(hrefs):
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
