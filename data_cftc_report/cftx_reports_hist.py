from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import sys

report_url = "https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm"
page = requests.get(report_url)
soup = bs(page.text, 'html.parser')

def get_report_links(url,reports):
    response = requests.get(url)
    page = response.content
    soup = bs(page)
    # all avaliable links 
    raw_links = []
    for lk in soup.findAll("a"):
        print(lk)
        raw_links.append(lk.attrs)
    return raw_links
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
all_reports_links = get_report_links(report_url, "/dea")
