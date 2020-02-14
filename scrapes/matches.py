from bs4 import BeautifulSoup
import requests

# todo: needs completing. JS is a limitation on these pages maybe Selinium or Scrapy required?
def scrape(matchID):
    url = f'http://en.espn.co.uk/statsguru/rugby/match/{matchID}.html'
    #response = requests.get(url)
    response = open('../html/match.html')
    soup = BeautifulSoup(response, 'lxml')
    val = soup.find('div', {'class': 'liveMthSubNavBG'})[0].find('td', {'class': 'liveSubNavText'}).text
    print(val)

#scrape('301907')
