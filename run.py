"""run.py
The main run script for the ETL process. Requires correctly set up database and .sql files for building each table.
This process currently not set for multi processes, but the server can handle such performance.
"""
import database
from scrapes import players, playerStats
from datetime import datetime as dt


def checkTables(tables):
    """
    :param tables: table to check the existence of
    :return: if the table is not present on the database it will be built during this process with relevant .sql file
    """
    for t in tables:
        if database.missingTable(t):
            buildsql = open(f'buildTools/{t}.sql').read().strip()
            database.executeQuery(buildsql)
            print(f'Table missing: building {t}:{dt.now()}')
        else:
            print(f'{t} present')


def scrapePlayers(playerid):
    """ Runs 2 seperate scrapes for each of the tables for each playerID
    :param playerid: the id of the player to get results from
    :return: the summary stats are added to both the player and playerStats tables
    """
    print(f'updating playerstats {playerid}:{dt.now()}')
    playerStats.scrape(playerid)
    print(f'updating player {playerid}:{dt.now()}')
    players.scrape(playerid)
    print(f'Finished {playerid}:{dt.now()}')


# todo: not started tables: Matches, match stats, Stadiums


if __name__ == '__main__':
    # EXAMPLE RUN
    # todo: set up process to run on more than just one playerID
    tables = ['playerstats', 'players']
    checkTables(tables)
    playerid = '11663'
    scrapePlayers(playerid)
