""" writeToHTML.py
This is just a development tool for saving a tempory file to practice scraping on
"""

from requests import get


def sampleScrape(url, file):
    response = get(url)
    f = open(f'html/{file}', "w")
    f.write(response.text)


sampleScrape('http://en.espn.co.uk/statsguru/rugby/player/11663.html',
       'html/playerExample.html')
