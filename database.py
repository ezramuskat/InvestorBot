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
# Methods to add data


def add_raw_13f_data_to_database(cik, quarter, holdings):
    """
    Takes in a cik, quarter, and list of holdings, and adds them to the database

    :param cik: The CIK of the company that the 13F is for
    :param quarter: The quarter of the filing
    :param holdings: a list of dictionaries, each dictionary representing a holding
    :return: The starting sequence for the IDs of all entries added by this method.
    """
    # create the format for the beginning of the id, and set up the format for the queries
    id_start = str(cik) + '-' + str(quarter) + '-'
    sql = "INSERT INTO `raw_13f_data` (`HoldingID`, `cik`, `quarter`, `issuer`, `cusip`, `class`, `value`, `shareprn_amount`, `shareprn_type`, `put_call`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    # go through each individual holding
    for holding in holdings:
        # add the holding to the database
        put_call = holding.get("putCall", None)
        holding_id = id_start + holding["cusip"]
        if put_call != None:
            holding_id = holding_id + '-' + put_call
        try:
            connection.cursor().execute(sql, (holding_id, cik, quarter, holding["nameOfIssuer"], holding["cusip"], holding[
                "titleOfClass"], holding["value"], holding["shrsOrPrnAmt"]["sshPrnamt"], holding["shrsOrPrnAmt"]["sshPrnamtType"], put_call))
            connection.commit()
        except pymysql.err.IntegrityError:
            print("Entry with id %s is already in the database", (holding_id))

    # Returning the id_start variable.
    return id_start


def populatedatabase(file):
    data = open(file, "r").read()
    asob = json.loads(data)
    count = 0
    for fil in asob['filings']:   # fil=fileing
        try:
            add_raw_13f_data_to_database(
                fil["cik"], fil["periodOfReport"], fil["holdings"])
            count = count + 1
            print(count)
        except KeyError:
            print("filing was missing a field")
            pass


def update_data_base():
    data = get_top_fund13f.get_last_quarter()
    asob = json.loads(data)
    for fil in asob['filings']:
        add_raw_13f_data_to_database(
            fil["cik"], fil["periodOfReport"], fil["holdings"])


def update_column_from_file(file, column):
    data = open(file, "r").read()
    asob = json.loads(data)
    count = 0
    for fil in asob['filings']:   # fil=fileing
        try:
            update_column(fil["cik"], fil["periodOfReport"],
                          fil["holdings"], column)
            count = count + 1
            print(count)
        except KeyError:
            print("filing was missing a field")
            pass


def update_column(cik, quarter, holdings, column):
    id_start = str(cik) + '-' + str(quarter) + '-'
    sql = ("UPDATE raw_13f_data SET {} = %s WHERE HoldingID = %s").format(column)
    # go through each individual holding
    for holding in holdings:
        # add the holding to the database
        put_call = holding.get("putCall", None)
        holding_id = id_start + holding["cusip"]
        if put_call != None:
            holding_id = holding_id + '-' + put_call
        connection.cursor().execute(
            sql, (holding["value"], holding_id))
        connection.commit()

    # Returning the id_start variable.
    return id_start

    # Methods to pull data


def get_total_stock_per_cik():
    """
    Returns a list of tuples, where each tuple contains the cik and the total number of stocks that
    the cik owns
    :return: A list of tuples. Each tuple contains the cik and the number of stocks that cik owns.
    """
    cursor = connection.cursor()
    cursor.execute(
        "SELECT cik, COUNT(cusip) FROM raw_13f_data  WHERE put_call is NULL GROUP BY cik ORDER BY cik DESC")
    return cursor.fetchall()


def get_total_unique_stock_per_cik():
    """
    Returns a list of tuples, where each tuple contains the cik and the number of unique stocks that
    the cik has filed for
    :return: A list of tuples. Each tuple contains the cik and the number of unique stocks that the cik
    has.
    """
    cursor = connection.cursor()
    cursor.execute(
        "SELECT cik, COUNT(DISTINCT cusip) FROM raw_13f_data  WHERE put_call is NULL GROUP BY cik ORDER BY cik DESC")
    return cursor.fetchall()


def get_unique_hedge_funds():
    cursor = connection.cursor()
    cursor.execute(
        "SELECT COUNT(DISTINCT cik) FROM raw_13f_data")
    return cursor.fetchone()[0]


def get_quarters():
    cursor = connection.cursor()
    cursor.execute(
        "SELECT DISTINCT quarter FROM raw_13f_data ORDER BY quarter DESC")

    return [quarter[0] for quarter in cursor.fetchall()]


def get_count_of_total_unique_holdings():
    cursor = connection.cursor()
    cursor.execute(
        "SELECT COUNT(DISTINCT cusip) FROM raw_13f_data WHERE NOT excluded AND put_call is NULL AND shareprn_type ='SH'")

    return cursor.fetchone()[0]


def get_all_unique_holdings():
    cursor = connection.cursor()
    cursor.execute(
        "SELECT DISTINCT cusip FROM raw_13f_data WHERE NOT excluded AND put_call is NULL AND shareprn_type ='SH'")

    return [holding[0] for holding in cursor.fetchall()]


def get_stock_names_from_cusips(cusip_list):
    cursor = connection.cursor()
    cusip_selectors = "(cusip = %s" + ("OR cusip = %s" *
                                       (len(cusip_list) - 1)) + ")"
    cursor.execute(
        "SELECT DISTINCT issuer FROM raw_13f_data WHERE NOT excluded AND put_call is NULL AND shareprn_type ='SH' AND " + cusip_selectors + " GROUP BY cusip", cusip_list)

    return [stock[0] for stock in cursor.fetchall()]
