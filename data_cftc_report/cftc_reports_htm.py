from bs4 import BeautifulSoup as bs
import requests
import os 
import sys


def get_report_links(reports):
    url = "https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm"
    response = requests.get(url)
    page = response.content
    soup = bs(page)
    # all avaliable links 
    raw_links = []
    for lk in soup.findAll("a"):
        print(lk)
        raw_links.append(lk.attrs)
    
    # all report links
    reports_links = []
    for rlk in raw_links:
        try: 
            web = rlk["href"]
        except KeyError:
            web = []
            pass 
        if web[:4] == reports:
            reports_links.append("https://www.cftc.gov"+web)
        else:
            pass
    return reports_links

# report "/dea/futures/deacmesf.htm"
all_reports_links = get_report_links("/dea")


report_url = "https://www.cftc.gov/dea/newcot/deafut.txt"
page = requests.get(report_url)
soup = bs(page.text, 'html.parser')
soup2 = soup.text

