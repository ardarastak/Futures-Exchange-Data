# -*- coding: utf-8 -*-
"""
Created on Wed Feb 26 12:37:57 2020

@author: orastak
"""

#%%get daily data via scraping website
import requests
import pandas as pd
from datetime import datetime
today = datetime.today().date()
url = "https://www.borsaistanbul.com/en/data/data/viop-derivatives-market/viop-delayed-quotes"
html = requests.get(url, verify=False).content
df_list = pd.read_html(html)
df30= df_list[0]
today1=[today]*len(df30)
df30.insert(0,"date",today1,True)
#%%get table that created by daily scraped data
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("mssql+pyodbc://user:password@server/database?driver=SQL+Server+Native+Client+11.0")
df31= pd.read_sql('SELECT * from VIOV',engine)
#%%concat tables 
df32=pd.concat([df31,df30],ignore_index=True)
#%%insert concated table to sql
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("mssql+pyodbc://user:password@server/database?driver=SQL+Server+Native+Client+11.0")
df32.to_sql('VIOV', con = engine, if_exists = 'replace', index=False)
#insert old data as backup table in case that web site is broken or data is corrupted
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("mssql+pyodbc://user:password@server/database?driver=SQL+Server+Native+Client+11.0")
df31.to_sql('VIOV_yedek', con = engine, if_exists = 'replace', index=False)
#%% Last 2 characters of Contract Code gives year of contract
import copy
df1=copy.deepcopy(df32) 
df1["year"]=[None]*len(df1)
for i in range(len(df1)):
    df1.loc[i,"year"]=int("20"+df1["Contract Code"][i][-2:])
#%% [-4:-2] of Contract Code gives month
def last4(s):
    return s[-4:-2]
df1["month1"]=[None]*len(df1)
for i in range(len(df1)):
    df1.loc[i,"month1"]=last4(df1.loc[i,"Contract Code"])
#%%  
b=[]
for i in range(len(df1)):
    a=[(df1["month1"][i][-2:-1]!="Q")&(df1["month1"][i][-2:]!="SY")]
    b.append(a)
#%%  
c=[]
for i in range(len(df1)):
    a=[(df1["month1"][i][-2:-1]=="Q")]
    c.append(a)
#%%  
d=[]
for i in range(len(df1)):
    a=[(df1["month1"][i][-2:]=="SY")]
    d.append(a)
#%% 
df1["type"]=[None]*len(df1)
for i in range(0,len(df1)):
    if b[i]==[True]:
        df1["type"][i]="monthly"
    elif c[i]==[True]:
        df1["type"][i]="quarterly"
    else:
        df1["type"][i]="yearly"
#%%
df1.loc[df1.Ask == 0, 'Ask'] = None
df1.loc[df1.Bid == 0, 'Bid'] = None
df1.loc[df1.Last == 0, 'Last'] = None
#%%          
#insert to sql
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("mssql+pyodbc://user:password@server/database?driver=SQL+Server+Native+Client+11.0")
df1.to_sql('VIOV_modified', con = engine, if_exists = 'replace', index=False)