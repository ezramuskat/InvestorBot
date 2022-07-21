from dotenv import load_dotenv
import os
import pymysql
import get_top_fund13f
import json
import logging
import recommender
import database
import sys
import time
load_dotenv()

connection = pymysql.connect(
    host=os.environ.get("DB_HOST"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    database=os.environ.get("DB_NAME")
)

def finle_eval(n=sys.maxsize):
    v =n
    if n >len(database.get_quarters()):
        v=len(database.get_quarters())
    whights=recommender.generate_weights(v-1)
    
    x=1+len(database.get_quarters())-v
    out= dict()
    for w in whights:
        rq =rank1(amountinvest(x,x))
        print( time.perf_counter(),"t")
        for stockr in rq:
            out.setdefault(stockr,0)
            out[stockr]=out[stockr]+rq[stockr]*w
        print(x)
        x= x+1
    return rank2(out)



def rank1(d):
   
    out = dict()
    listt = sorted(d.items(), key=lambda x:x[1])
    sortdict = dict(listt)
    
    res =dict(reversed(list(sortdict.items())))
    i=len(res)
    for it in res:
        out[it]=i
        i=i-1
    
    return out
def rankorder(d):
    out = dict()
    listt = sorted(d.items(), key=lambda x:x[1])
    sortdict = dict(listt)
    res =dict(reversed(list(sortdict.items())))
    return res

def rank2(d):
    
    out = dict()
    listt = sorted(d.items(), key=lambda x:x[1])
    sortdict = dict(listt)
    
    res =dict(reversed(list(sortdict.items())))
    i=1
    for it in res:
        out[it]=i
        i=i+1
    return out


def percentbracet(dic):
    ordered =rankorder(dic)
    out = dict()
    uperbraket =int()
    lower =int()
    first = True
    for stock in ordered:
        if first:
            first=False
            lower=int(ordered[stock])
            out[lower]=0
        if ordered[stock]>=lower:
            out[lower]=out[lower]+1
        else:
            lower= int(ordered[stock])
            out[lower]=1
    return out





def amountinvest(to,frm):# spesifiy between wich quorter you want the results range  ex if you want (5,5) it will gve you five if you want (5,6) it will give avrege between 5 and 6
    t =time.perf_counter()
    x = (1167483 , 1647251 , 1009207 , 1061768 , 1656456 , 1273087 , 1423053 , 1581811 , 909661, 1061165, 1103804, 1350694 ,  1791786, 1040273 )
    out = dict()
    cursor4 = connection.cursor()
    cursor4.execute("SELECT DISTINCT(quarter) FROM raw_13f_data WHERE  put_call is NULL ORDER BY quarter DESC")
    quartes = cursor4.fetchall()
    t =time.perf_counter()
    cursor2 = connection.cursor()
    cursor2.execute("SELECT cusip, value, cik FROM raw_13f_data WHERE quarter="+"'"+quartes[to-1][0]+"'"+" AND shareprn_type = 'SH' AND put_call is NULL")
    q2 = cursor2.fetchall()
    for fund in x:
        tempdict=dict()
        totalval=0
        for stock in q2:
            if stock[2]==fund:
                tempdict.update({stock[0]:stock[1]})
                totalval=totalval+stock[1]    
        for st in tempdict:
            tempdict[st]=tempdict[st]*100/totalval
            out.setdefault(st,0)
            out[st]=out[st]+(tempdict[st]/len(x))
        
            

        
    print( time.perf_counter()-t,"th")
    return out
            
                
