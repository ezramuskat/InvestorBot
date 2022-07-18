import database
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

connection = pymysql.connect(
    host=os.environ.get("DB_HOST"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    database=os.environ.get("DB_NAME")
)


def get_stock_percentages():
    """
    Returns a dictionary of quarters and the percentage of filings in each quarter that have a given holding
    :return: A dictionary of tuples.
    """
    cursor = connection.cursor()
    percentages = {}
    quarters = database.get_quarters()

    for quarter in quarters:
        cursor.execute(
            "SELECT cusip, (COUNT(DISTINCT cik)/14 * 100) AS PERCENTAGE FROM raw_13f_data WHERE quarter = %s AND put_call is NULL AND shareprn_type ='SH' AND NOT excluded GROUP BY cusip;", (quarter))
        percentages[quarter] = cursor.fetchall()

    return percentages
