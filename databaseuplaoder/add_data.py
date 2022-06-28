import database
import get_to_fundf13
import json

def populatedatabase():
    data = get_to_fundf13.get_historical_data()
    asob = json.loads(data)
    for fil in asob['filings']:   # fil=fileing
        database.add_raw_13f_data_to_database(fil["cik"],fil["periodOfReport"],fil["holdings"])

def update_data_base():
    data = get_to_fundf13.get_last_quarter():
    asob = json.loads(data)
    for fil in asob['filings']:
        database.add_raw_13f_data_to_database(fil["cik"],fil["periodOfReport"],fil["holdings"])

