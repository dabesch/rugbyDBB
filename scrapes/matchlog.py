import requests
from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd


def readNotes(notes, matchID):
    notesDict = {'matchID': matchID}
    teams = ['Home', 'Away']
    tid = 0
    for n in notes:
        res = n.text.strip().split('\n')
        if len(res) > 1:
            title = res[0]
            if 'debuts' in title:
                title = f'{teams[tid]} debuts'
                tid += 1
            values = "".join(res[1:])
            if title == 'Referee' or title == 'Ground name':
                href = n.a['href'].split('/')[-1].split('.')[0]
                notesDict[f'{title}ID'] = int(href)
            notesDict[title] = values
    return notesDict


# todo output needs finalizing, currently 2 dictionaries
def readTeams(teams):
    homestats = {'home': True}
    awaystats = {'home': False}
    for t in teams.find_all('tr'):
        if not t.find('div', {'class': 'divTeams'}):
            items = t.text.strip().split('\n')
            if len(items) > 1:
                sp = int(len(items) / 2)
                if sp == 1:
                    homestats['team'] = items[:sp][0]
                    awaystats['team'] = items[sp:][0]
                else:
                    homestats[items[sp:][0]] = items[:sp][1].strip()
                    awaystats[items[sp:][0]] = items[sp:][1].strip()
        else:
            break
    teamDict = {'home': homestats, 'away': awaystats}
    return teamDict


def parseScore(string, matchID, nation, sType):
    scD = []
    for sc in string.split('),'):
        results = sc.split(' (')
        scorer = results[0].strip()
        scores = results[1].split(',')

        if len(scores) > 1:
            for s in scores:
                scD.append([matchID, nation, scorer, sType, int(s.strip(')'))])
        else:
            scD.append([matchID, nation, scorer, sType, int(scores[0].strip(')'))])
    return scD


def matchStats(matchStats):
    teams = matchStats.find_all('td', {'class': 'liveMthHdrGrn'})
    teams = [t.text for t in teams]
    stats = {'team': teams, 'home': [1, 0]}
    for m in matchStats.find_all('tr'):
        if len(m.find_all('td', {'class': 'liveTblTextGrn'})) == 1:
            title = m.find('td', {'class': 'liveTblTextGrn'}).text
            blktxt = m.find_all('td', {'class': 'liveTblColCtr'})
            txt = [b.text.strip() for b in blktxt]

            stats[title] = txt
    return processTeamStats(stats)


def processTeamStats(stats):
    stats['tries'] = [t[0] for t in stats['Tries']]
    stats['conversion_attempts'], stats['conversion_goals'] = [c.split(' from') for c in stats['Conversion goals']]
    stats['penalty_attempts'], stats['penalty_goals'] = [c.split(' from') for c in stats['Penalty goals']]
    stats['yellow_cards'], stats['red_cards'] = [c.split('/') for c in stats['Yellow/red cards']]
    stats['tackles_made'], stats['tackles_missed'] = [c.split('/') for c in stats['Tackles made/missed']]

    stats['possession_overall'] = [s.split(' ')[0] for s in stats['Possession (1H/2H)']]
    stats['possession_1H'] = [s[s.find("(") + 1:s.find(")")].split('/')[0] for s in stats['Possession (1H/2H)']]
    stats['possession_2H'] = [s[s.find("(") + 1:s.find(")")].split('/')[1] for s in stats['Possession (1H/2H)']]

    stats['territory_overall'] = [s.split(' ')[0] for s in stats['Territory (1H/2H)']]
    stats['territory_1H'] = [s[s.find("(") + 1:s.find(")")].split('/')[0] for s in stats['Territory (1H/2H)']]
    stats['territory_2H'] = [s[s.find("(") + 1:s.find(")")].split('/')[1] for s in stats['Territory (1H/2H)']]

    stats['rucks'] = [s.split(' ')[2] for s in stats['Rucks won']]
    stats['rucks_won'] = [s.split(' ')[0] for s in stats['Rucks won']]
    stats['mauls'] = [s.split(' ')[2] for s in stats['Mauls won']]
    stats['mauls_won'] = [s.split(' ')[0] for s in stats['Mauls won']]
    stats['scrums'] = [s.split(' ')[2] for s in stats['Scrums on own feed']]
    stats['scrums_won'] = [s.split(' ')[0] for s in stats['Scrums on own feed']]
    stats['lineouts'] = [s.split(' ')[2] for s in stats['Lineouts on own throw']]
    stats['lineouts_won'] = [s.split(' ')[0] for s in stats['Lineouts on own throw']]
    stats['penalties_conceded'] = [s.split(' ')[0] for s in stats['Penalties conceded (Freekicks)']]

    for ky in ['Tries', 'Yellow/red cards', 'Tackles made/missed', 'Conversion goals', 'Penalty goals',
               'Possession (1H/2H)', 'Territory (1H/2H)', 'Scrums on own feed', 'Lineouts on own throw', 'Rucks won',
               'Mauls won', 'Penalties conceded (Freekicks)']:
        stats.pop(ky, None)
    return stats


def teamStats(teamStats, matchID, nation):
    statString = []
    for row in teamStats.find_all('tr'):
        if not row.find('td', {'colspan': '14'}):
            statString.append(row.text.replace('\n', ',').strip(','))
        else:
            break

    csv = StringIO('\n'.join(statString))
    df = pd.read_csv(csv).drop(['Unnamed: 10'], axis=1)
    df.rename(columns={' ': 'initName',
                       'Pts': 'points',
                       'MR': 'distance_run',
                       'CB': 'clean_breaks',
                       'DB': 'defenders_beaten',
                       'OL': 'offloads',
                       'TO': 'turn_overs',
                       'Pen': 'penalties_conceded'},
              inplace=True)
    df['match_id'] = matchID
    df['nation'] = nation

    clean = {'T/A': ['tries', 'try_assists'],
             'K/P/R': ['kicks', 'passes', 'runs'],
             'Tack': ['tackles_made', 'tackles_missed'],
             'LO': ['lineouts_won', 'lineouts_stolen'],
             'Y/R': ['yellows', 'reds']}

    for key in clean.keys():
        new = df[key].str.split('/', expand=True)
        n = 0
        for col in clean[key]:
            df[col] = new[n]
            n += 1
    df.drop([k for k in clean.keys()], axis=1, inplace=True)

    return df


matchID = 300729
file = f'../html/Rugby Union - ESPN Scrum - Wales v Ireland at Millennium Stadium_files/{matchID}.html'
response = open(file).read()
# response = requests.get(url).text
soup = BeautifulSoup(response, 'lxml')

left = soup.find('div', {'id': 'liveLeft'})
right = soup.find('div', {'id': 'liveRight'})

ltabs = left.find_all('div', {'class': 'tabbertab'})
rtabs = right.find_all('div', {'class': 'tabbertab'})

team = 0
notesCheck = 0
teamCheck = 0
matchStatsCheck = 0
teamStatsCheck = 0

# checks both sides as layouts vary
for side in [ltabs, rtabs]:

    for tab in side:
        if tab.h2.text == 'Notes':
            notes = readNotes(tab.find_all('tr'), matchID)
            notesCheck += 1
        elif tab.h2.text == 'Teams':
            teams = readTeams(tab.find('table'))
            teamCheck += 1
        elif tab.h2.text == 'Match stats':
            mStats = matchStats(tab.find('table'))
            matchStatsCheck += 1
        elif 'stats' in tab.h2.text:
            nation = tab.h2.text.split(' ')[0]
            tStats = teamStats(tab.find('table'), matchID, nation)
            teamStatsCheck += 1

resultsLookup = {
    'notesCheck': notesCheck,
    'teamCheck': teamCheck,
    'matchStatsCheck': matchStatsCheck,
    'teamStatsCheck': teamStatsCheck
}

print(resultsLookup)

print(notes)
print(teams)
print(mStats)
print(tStats)
