from dotenv import load_dotenv
import os
import pymysql
import get_top_fund13f
import json
import logging
import recommender
load_dotenv()

connection = pymysql.connect(
    host=os.environ.get("DB_HOST"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    database=os.environ.get("DB_NAME")
)

def finle_eval():
    whights=recommender.generate_weights(11)
    x=1
    out= dict()
    for w in whights:
        rq =rank1(amountinvest(x,x))
        for stockr in rq:
            out.setdefault(stockr,0)
            out[stockr]=out[stockr]+rq[stockr]*w
        print()
        x= x+1
    return out



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
    i=0
    for it in res:
        out[i]=it
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
    x = (1647251 , 1423053 , 1009207 , 1273087 , 1791786 , 1350694 , 1061768 , 909661 , 1040273 , 1581811 , 1656456 , 1218199 ,  1103804 , 1061165 , 1167483 )
    out = dict()
    for val in x:
        cursor4 = connection.cursor()
        cursor4.execute("SELECT DISTINCT(quarter) FROM raw_13f_data WHERE  put_call is NULL ORDER BY quarter DESC")
        quartes = cursor4.fetchall()
        
        qort=frm-1
        qurtdict=dict()
        while qort>=to-1:
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
                qurtdict[st]=qurtdict[st]+(tempdict[st]/(frm-to+1))

        for sto in qurtdict.keys():
            out.setdefault(sto,0)
            out[sto]=out[sto] + (qurtdict[sto]/len(x))
    return out
            
                
