from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO
report_url = "https://www.cftc.gov/dea/newcot/deafut.txt"
page = requests.get(report_url)
soup = bs(page.text, 'html.parser')
report_csv = StringIO(soup.text)
df = pd.read_csv(report_csv)
df.iloc[0] = df.columns.tolist()
df.columns = list(range(0,len(df.columns.tolist())))
butter = df.iloc[84]