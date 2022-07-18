from dotenv import load_dotenv
import os
import pymysql
import get_top_fund13f
import json
import logging

load_dotenv()

connection = pymysql.connect(
    host=os.environ.get("DB_HOST"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    database=os.environ.get("DB_NAME")
)
def rank1(d):
    out = dict()
    listt = sorted(d.items(), key=lambda x:x[1])
    sortdict = dict(listt)
    print(sortdict)
    r = len(sortdict)
    res =dict(reversed(list(sortdict.items())))
    i=0
    for it in res:
        out[it]=i
        i=i+1
    return out
def rankorder(d):
    out = dict()
    listt = sorted(d.items(), key=lambda x:x[1])
    sortdict = dict(listt)
    print(sortdict)
    r = len(sortdict)
    res =dict(reversed(list(sortdict.items())))
    return res    

def amountinvest(n):
    x = (1647251 , 1423053 , 1009207 , 1273087 , 1791786 , 1350694 , 1061768 , 909661 , 1040273 , 1581811 , 1656456 , 1218199 ,  1103804 , 1061165 , 1167483 )
    out = dict()
    for val in x:
        cursor4 = connection.cursor()
        cursor4.execute("SELECT DISTINCT(quarter) FROM raw_13f_data WHERE cik = " + str(val)+" AND put_call is NULL ORDER BY quarter DESC")
        quartes = cursor4.fetchall()
        
        qort=n-1
        qurtdict=dict()
        while qort>=0:
            cursor2 = connection.cursor()
            cursor2.execute("SELECT cusip, value FROM raw_13f_data WHERE cik = " + str(val)+" AND quarter="+"'"+quartes[qort][0]+"'"+" AND shareprn_type = 'SH' AND put_call is NULL")
            q2 = cursor2.fetchall()
            qort=qort-1
            totalval=0
            tempdict=dict()
            for stock in q2:
               tempdict.update({stock[0]:stock[1]})
               totalval=totalval+stock[1]
            for st in tempdict.keys():
                tempdict[st]= tempdict[st]*100/totalval
                qurtdict.setdefault(st,0)
                qurtdict[st]=qurtdict[st]+(tempdict[st]/n)

        for sto in qurtdict.keys():
            out.setdefault(sto,0)
            out[sto]=out[sto] + (qurtdict[sto]/len(x))
    return out
            
                
