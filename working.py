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

def calc1():
    cursor = connection.cursor()
    cursor.execute("SELECT cik FROM raw_13f_data")
    x = cursor.fetchall()
    return x

def calc2():
    
    cursor = connection.cursor()
    cursor.execute("SELECT cik FROM raw_13f_data")
    x = (1647251 , 1423053 , 1009207 , 1273087 , 1791786 , 1350694 , 1061768 , 909661 , 1040273 , 1581811 , 1595082 , 1656456 , 1218199 , 1603466 , 1103804 , 441534605400 , 1061165 , 1167483 , 1029160)
    rout= dict()
   
    for val in x:
        cursor4 = connection.cursor()
        cursor4.execute("SELECT DISTINCT(quarter) FROM raw_13f_data WHERE cik = " + str(val)+" AND put_call is NULL ORDER BY quarter DESC")
        quartes = cursor4.fetchall()
        
        cursor = connection.cursor()
        cursor.execute("SELECT DISTINCT(cusip) FROM raw_13f_data WHERE cik = " + str(val)+" AND shareprn_type = 'SH' AND put_call is NULL")
        q = cursor.fetchall()
    #do counting here
        streck={}
       
        fileout=0
        led=0
        for cu in q:
            stock =0
            counter=0
            cursor = connection.cursor()
            cursor.execute("SELECT cusip, quarter FROM raw_13f_data WHERE cik = " + str(val)+" AND cusip = " +"'"+str(cu[0])+"'"+" AND shareprn_type = 'SH' AND put_call is NULL ORDER BY quarter DESC")
            z = cursor.fetchall()
            i=0
            ons=False
            for t in z:
                
                if t[1]== quartes[i][0]:
                    stock=stock+1
                    ons=True
                else:
                    if stock>=1:
                        if ons:
                            counter=counter+1
                            ons=False
                i=i+1
            if (counter == 0):
                counter= 1
            ot=stock/counter
            
            fileout=ot+fileout
            led=led+1
        out=fileout/led
        print(out)
        rout.update({val:out})
    return rout


def calc2_2():# calc 2 was taking to long this is a slightly difrent calculation but should
    # tell the same thing but mutch faster   this is avregestreek lenth  aka allstreecs / number of streeks
    # calc2 is avrege of (agvrege of(streeklenth perstock))
    cursor = connection.cursor()
    cursor.execute("SELECT cik FROM raw_13f_data")
    x = (1647251 , 1423053 , 1009207 , 1273087 , 1791786 , 1350694 , 1061768 , 909661 , 1040273 , 1581811 , 1595082 , 1656456 , 1218199 , 1603466 , 1103804 , 1061165 , 1167483 , 1029160)
    out = dict()
    for val in x:
        cursor4 = connection.cursor()
        cursor4.execute("SELECT DISTINCT(quarter) FROM raw_13f_data WHERE cik = " + str(val)+" AND put_call is NULL ORDER BY quarter DESC")
        quartes = cursor4.fetchall()
        size = len(quartes)-1
        totdroped=0
        alls=0
        numbers=0
        dictofs= dict()
        while size>=0 :
            cursor5 = connection.cursor()
            cursor5.execute("SELECT DISTINCT(cusip) FROM raw_13f_data WHERE cik = " + str(val)+" AND quarter="+"'"+quartes[size][0]+"'"+" AND shareprn_type = 'SH' AND put_call is NULL")
            q1 = cursor5.fetchall()
            size= size-1
            if size<0:
                q2=tuple()
            else:
            
                cursor6 = connection.cursor()
                cursor6.execute("SELECT DISTINCT(cusip) FROM raw_13f_data WHERE cik = " + str(val)+" AND quarter="+"'"+quartes[size][0]+"'"+" AND shareprn_type = 'SH' AND put_call is NULL")
                q2 = cursor6.fetchall()
            for sto in q1:
                dictofs.setdefault(sto[0],0)
                dictofs[sto[0]]=dictofs[sto[0]]+1
                
                if sto in q2:
                    iiiiiiiiii=9# i know this is badly writen i dont care
                else:
                    numbers=numbers+1
                    alls =alls+dictofs[sto[0]]
                    dictofs[sto[0]]=0
                
            

        
        out.update({val:alls/numbers})
        print(out)
    return out






def calc3():
    out= dict()
    cursor = connection.cursor()
    cursor.execute("SELECT cik FROM raw_13f_data")
    x = (1647251 , 1423053 , 1009207 , 1273087 , 1791786 , 1350694 , 1061768 , 909661 , 1040273 , 1581811 , 1595082 , 1656456 , 1218199 , 1603466 , 1103804 , 441534605400 , 1061165 , 1167483 , 1029160)
    allfunds= dict()
    for val in x:
        allfunds.update({val:dict()})
        cursor2 = connection.cursor()
        cursor2.execute("SELECT cusip, quarter FROM raw_13f_data WHERE cik = " + str(val)+" AND shareprn_type = 'SH' AND put_call is NULL ORDER BY quarter DESC")
        z = cursor2.fetchall()
        cursor3 = connection.cursor()
        cursor3.execute("SELECT DISTINCT(cusip) FROM raw_13f_data WHERE cik = " + str(val)+" AND put_call is NULL")
        q = cursor3.fetchall()

        for issue in z:
            allfunds[val].setdefault(issue[0],0)
            allfunds[val][issue[0]]=allfunds[val][issue[0]]+1
        counter = 0
        stocknumber=0
        for stock in allfunds[val].keys():
            counter+=1
            stocknumber+= allfunds[val][stock]
        if counter==0:
            i=-1
        else:
            i = stocknumber/counter
        out.update({val:i})
    return out
    #do counting 









def calc4():
    cursor = connection.cursor()
    cursor.execute("SELECT cik FROM raw_13f_data")
    x = (1647251 , 1423053 , 1009207 , 1273087 , 1791786 , 1350694 , 1061768 , 909661 , 1040273 , 1581811 , 1595082 , 1656456 , 1218199 , 1603466 , 1103804 , 1061165 , 1167483 , 1029160)
    out = dict()
    for val in x:
        cursor4 = connection.cursor()
        cursor4.execute("SELECT DISTINCT(quarter) FROM raw_13f_data WHERE cik = " + str(val)+" AND put_call is NULL ORDER BY quarter DESC")
        quartes = cursor4.fetchall()
        size = len(quartes)-1
        totdroped=0
        while size>0 :
            cursor5 = connection.cursor()
            cursor5.execute("SELECT DISTINCT(cusip) FROM raw_13f_data WHERE cik = " + str(val)+" AND quarter="+"'"+quartes[size][0]+"'"+" AND shareprn_type = 'SH' AND put_call is NULL")
            q1 = cursor5.fetchall()
            size= size-1
            cursor6 = connection.cursor()
            cursor6.execute("SELECT DISTINCT(cusip) FROM raw_13f_data WHERE cik = " + str(val)+" AND quarter="+"'"+quartes[size][0]+"'"+" AND shareprn_type = 'SH' AND put_call is NULL")
            q2 = cursor6.fetchall()
            drop=0
            for sto in q1:
                if sto in q2:
                    iiiiiiiiii=9# i know this is badly writen i dont care
                else:
                    drop=drop+1
                
            
            droppercent=(drop/len(q1))*100
            totdroped= totdroped+droppercent
        funddroprate=totdroped/(len(quartes)-1)
        print(funddroprate)
        print(val)
        out.update({val:funddroprate})
        print()
    return out
    #do counting 
