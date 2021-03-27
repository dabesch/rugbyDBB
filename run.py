"""run.py
The main run script for the ETL process. Requires correctly set up database and .sql files for building each table.
This process currently not set for multi processes, but the server can handle such performance.
"""
from datetime import datetime as dt

import database
from scrapes import players, player_stats, grounds


def check_tables(tables):
    """
    :param tables: table to check the existence of
    :return: if the table is not present on the database it will be built during this process with relevant .sql file
    """
    for t in tables:
        if database.missing_table(t):
            buildsql = open(f'buildTools/{t}.sql').read().strip()
            database.execute_query(buildsql)
            print(f'Table missing: building {t}:{dt.now()}')
        else:
            print(f'{t} present')


def scrape_players(player_id):
    """ Runs 2 separate scrapes for each of the tables for each playerID
    :param player_id: the id of the player to get results from
    :return: the summary stats are added to both the player and playerStats tables
    """
    print(f'updating playerstats {player_id}:{dt.now()}')
    player_stats.scrape(player_id)
    print(f'updating player {player_id}:{dt.now()}')
    players.scrape(player_id)
    print(f'Finished {player_id}:{dt.now()}')


# todo: not started tables: Matches, match stats


if __name__ == '__main__':
    # EXAMPLE RUN
    # todo: set up process to run on more than just one playerID
    # tables = ['playerstats', 'players', 'grounds']
    # checkTables(tables)
    # player_id = '11663'
    # scrapePlayers(player_id)

    grounds.scrape('16727')
    grounds.scrape('16145')
