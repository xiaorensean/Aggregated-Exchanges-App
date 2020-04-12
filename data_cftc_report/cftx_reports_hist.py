from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import sys

report_url = "https://www.cftc.gov/dea/newcot/deafut.txt"
page = requests.get(report_url)
soup = bs(page.text, 'html.parser')

