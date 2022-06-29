import json
import os
from sec_api import QueryApi
import datetime as dt
from dateutil.relativedelta import *
from dotenv import load_dotenv


def createQueryTextFromStartToEndTimeFormated(start_time, end_time):
    return '{"query": {"query_string": {"query": "cik:(1647251 OR 1423053 OR 1009207 OR 1273087 OR 1791786 OR 1350694 OR 1061768 OR 909661 OR 1040273 OR 1581811 OR 1595082 OR 1656456 OR 1218199 OR 1603466 OR 1103804 OR 441534605400 OR 1061165 OR 1167483 OR 1029160) AND NOT formType:\"13F-HR/A\" AND formType:\"13F-HR\" AND filedAt:[' + \
        start_time + ' TO ' + end_time + \
        ']","time_zone": "America/New_York"}},"from": "0","size": "50","sort": [{"filedAt": {"order": "desc"}}]}'


# Get the current time as a DateTime object
time_and_date_now = dt.datetime.now()
# Format the current time into the SEC-API.io API time format
formated_current_time = time_and_date_now.strftime("%Y-%m-%dT%H:%M:%S")
# This system has two steps: 1. the original data pull and 2.the quarterly pull of new data as it is posted on the SEC EDGAR database

# 1. First, pull all the data starting at some number of years. In our case, we pull the data for the last seven years.


def getDataForThePastQuarters(number_of_quarters):
    # Calulate how many months back to look for 13F reports
    #  3 months is a quarter, then subtract another 60 days to offset by the time allotted for the hedge funds to release their data about the previous quarter.
    month_shift = (number_of_quarters * 3) + 2
    start_time = time_and_date_now + relativedelta(months=-month_shift)
    start_time_formated = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    return createQueryTextFromStartToEndTimeFormated(start_time_formated, formated_current_time)


# generate date_of_original_data
# 2.Update by getting the most recent data after the initial pull(need )
# start from date_of_original_data and go until curent_date


def getF13DataSinceDate(year, month, day):
    start_time = dt.date(year, month, day)
    start_time_formated = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    return createQueryTextFromStartToEndTimeFormated(start_time_formated, formated_current_time)


def getF13DataSinceDateTimeObject(start_time):
    start_time_formated = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    return createQueryTextFromStartToEndTimeFormated(start_time_formated, formated_current_time)


# this will take a JSON filing for a single fund and single filing and will
# return that fund as a JSON that will have name date and holdings


def get_single_f13releventdata(unfiltered_f13):
    f13_filtered = unfiltered_f13["cik"]
    as_objsect = json.loads('{}')   # creats an object
    as_objsect.update({"cik": unfiltered_f13["cik"]})
    as_objsect.update(
        {"periodOfReport": unfiltered_f13["periodOfReport"]})  # adds =update
    as_objsect.update({"holdings": unfiltered_f13["holdings"]})
    return json.dumps(as_objsect)  # returns it back as a json string


# this will take a json of all the fileings and feed them to  get_single_f13 relevent data
# then it will combinde the results of the relevent data


def get_specific_f13(full_jsons_of_all):
    data = '{}'
    data_ob = json.loads(data)
    i = 0
    for fill in full_jsons_of_all['filings']:
        data_ob.update({i: get_single_f13releventdata(fill)})
        i = i+1
    return json.dumps(data_ob)


def queryget(query):
    load_dotenv()
    queryApi = QueryApi(
        api_key=os.environ.get("SEC-API_Key"))
    return get_specific_f13(queryApi.get_filings(query))


# 1


def get_historical_data():  # return historical data of past 7 years
    return queryget(getDataForThePastQuarters(35))


# 2


def get_last_quarter():
    return queryget(getDataForThePastQuarters(1))
