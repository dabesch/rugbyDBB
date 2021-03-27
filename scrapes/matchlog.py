from io import StringIO

import pandas as pd
from bs4 import BeautifulSoup

from database import createSQL, executeQuery, writeDataFrame


def read_notes(notes, matchID):
    """
    :param notes: Soup object which contains results from the 'Notes' tab
    :param matchID: the match id
    :return: returns a complete dictionary of the notes from the match
    """
    notes_dict = {'match_id': matchID}

    for n in notes:
        res = n.text.strip().split('\n')
        if len(res) > 1:
            title = res[0].replace(' ', '_')
            if 'debuts' in title:
                continue
            values = "".join(res[1:])
            if title == 'Referee' or title == 'Ground_name':
                href = n.a['href'].split('/')[-1].split('.')[0]
                notes_dict[f'{title}_id'] = int(href)
            notes_dict[title] = values
        elif 'Attendance' in res[0]:
            notes_dict['attendance'] = res[0].split(' ')[-1]

    return notes_dict


# todo output needs finalizing, currently 2 dictionaries
def read_teams(teams):
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
    team_dict = {'home': homestats, 'away': awaystats}
    return team_dict


# todo not yet used but should be used with readTeams above
def parse_score(string, matchID, nation, sType):
    score_dict = []
    for sc in string.split('),'):
        results = sc.split(' (')
        scorer = results[0].strip()
        scores = results[1].split(',')

        if len(scores) > 1:
            for s in scores:
                score_dict.append([matchID, nation, scorer, sType, int(s.strip(')'))])
        else:
            score_dict.append([matchID, nation, scorer, sType, int(scores[0].strip(')'))])
    return score_dict


def match_stats(matchStats, teamScores, matchID):
    """
    :param matchStats: soup object which contains the match stats for the overall game
            (separate from individual team stats)
    :return: a dictionary object which is then cleaned and converted to a DataFrame with processMatchStats()
    """
    teams = [teamScores[t]['team'] for t in ['home', 'away']]
    stats = {'team': teams, 'home': [1, 0]}

    for m in matchStats.find_all('tr'):
        if len(m.find_all('td', {'class': 'liveTblTextGrn'})) == 1:
            title = m.find('td', {'class': 'liveTblTextGrn'}).text
            blktxt = m.find_all('td', {'class': 'liveTblColCtr'})
            txt = [b.text.strip() for b in blktxt]

            stats[title] = txt
    return process_match_stats(stats, matchID)


def process_match_stats(stats, matchID):
    """

    :param stats: A dictionary object which contains the output of matchStats() but is not yet cleaned
    :return: a cleaned up pandas Dataframe, ready for writing to the database
    """
    stats['tries'] = [t[0] for t in stats['Tries']]
    stats['conversion_attempts'], stats['conversion_goals'] = [c.split(' from') for c in stats['Conversion goals']]
    stats['penalty_attempts'], stats['penalty_goals'] = [c.split(' from') for c in stats['Penalty goals']]
    stats['yellow_cards'], stats['red_cards'] = [c.split('/') for c in stats['Yellow/red cards']]
    stats['tackles_made'] = [c.split('/')[0] for c in stats['Tackles made/missed']]
    stats['tackles_missed'] = [c.split('/')[1] for c in stats['Tackles made/missed']]

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
               'Mauls won', 'Penalties conceded (Freekicks)', 'Tackling success rate']:
        stats.pop(ky, None)

    # Produce a DataFrame, rename columns and turn all percentages to floats
    stats = pd.DataFrame(stats)
    stats['match_id'] = matchID
    stats.columns = [col.replace(' ', '_') for col in stats.columns]
    for col in ['Kick_at_goal_success', 'possession_overall', 'possession_1H', 'possession_2H', 'territory_overall',
                'territory_1H', 'territory_2H']:
        stats[col] = stats[col].str.replace('%', '').astype('float') / 100

    return pd.DataFrame(stats)


def team_stats(teamStats, matchID, nation):
    """

    :param teamStats: soup object which contains the table of team stats
    :param matchID: the matchID, required for creating data
    :param nation: the nation which this is the stats for
    :return: returns a pandas dataframe object with the results
    """
    # creates a string which is then formatted into csv and read into a pandas DataFrame
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

    # The selected fields are all separated by '/' and therefore split into multiple columns for cleaner data
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

    # Cleaned and processed dataframe is produced
    return df


def timelineDetails(timeline):
    return timeline


def scrape(matchID):
    file = f'../html/Rugby Union - ESPN Scrum - Wales v Ireland at Millennium Stadium_files/{matchID}.html'
    response = open(file).read()
    # response = requests.get(url).text
    soup = BeautifulSoup(response, 'lxml')

    left = soup.find('div', {'id': 'liveLeft'})
    right = soup.find('div', {'id': 'liveRight'})

    ltabs = left.find_all('div', {'class': 'tabbertab'})
    rtabs = right.find_all('div', {'class': 'tabbertab'})

    notesCheck = 0
    teamCheck = 0
    matchStatsCheck = 0
    teamStatsCheck = 0
    timeline = 0

    # checks both sides as layouts vary
    for side in [ltabs, rtabs]:

        for tab in side:
            if tab.h2.text == 'Notes':
                notes = read_notes(tab.find_all('tr'), matchID)
                notesCheck += 1
            elif tab.h2.text == 'Teams':
                teamScores = read_teams(tab.find('table'))
                teamCheck += 1
            elif tab.h2.text == 'Match stats':
                mStats = match_stats(tab.find('table'), teamScores, matchID)
                matchStatsCheck += 1
            elif tab.h2.text == 'Timeline':
                timelineStats = tab.find('table')

                timeline += 1
            elif 'stats' in tab.h2.text:
                nation = tab.h2.text.split(' ')[0]
                if teamStatsCheck == 0:
                    tStats = team_stats(tab.find('table'), matchID, nation)
                else:
                    tStats = pd.concat([tStats, team_stats(tab.find('table'), matchID, nation)])
                teamStatsCheck += 1

    # log of reads completed
    resultsLookup = {'match_id': matchID,
                     'notes_check': notesCheck,
                     'timeline_check': timeline,
                     'team_check': teamCheck,
                     'match_stats_check': matchStatsCheck,
                     'team_stats_check': teamStatsCheck
                     }

    # todo: write teamScores & timeline to db. Change the way the response is called
    print(teamScores)
    print(timeline)
    executeQuery(createSQL(notes, 'match_notes'))
    executeQuery(createSQL(resultsLookup, 'match_details_log'))
    writeDataFrame(mStats, table='player_stats_match')
    writeDataFrame(tStats, table='team_match_stats')


# Run all the functions
matchID = '300729'
scrape(matchID)
