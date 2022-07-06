from dotenv import load_dotenv
from datetime import date
import os
import pymysql
import statistics
import database

load_dotenv()

connection = pymysql.connect(
    host=os.environ.get("DB_HOST"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    database=os.environ.get("DB_NAME")
)
cursor = connection.cursor()

file_name = "data-metrics-" + str(date.today()) + ".txt"

f = open(file_name, "w")

# Get number of unique hedge funds
f.write("Number of unique hedge funds: " +
        str(database.get_unique_hedge_funds()[0]) + "\n")

# Get data on number of filings per hedge fund
f.write("Filing data: " + "\n")
cursor.execute(
    "SELECT cik, COUNT(DISTINCT quarter) FROM raw_13f_data GROUP BY cik ORDER BY cik DESC")

filings_count_dict = dict(cursor.fetchall())

for cik in filings_count_dict:
    f.write(" " + str(cik) + ": " +
            str(filings_count_dict[cik]) + " filings" + "\n")

max_filings = max(filings_count_dict.values())
min_filings = min(filings_count_dict.values())
median_filings = statistics.median(filings_count_dict.values())
f.write("The maximum number of filings is " + str(max_filings) + "." + "\n")
f.write("The minimum number of filings is " + str(min_filings) + "." + "\n")
f.write("The median number of filings is " + str(median_filings) + "." + "\n")

# Get data on unique stocks per hedge fund

unique_stocks_dict = dict(database.get_total_unique_stock_per_cik())

f.write("Stock data: " + "\n")

for cik in unique_stocks_dict:
    f.write(" " + str(cik) + ": " +
            str(unique_stocks_dict[cik]) + " unique stocks" + "\n")

max_stocks = max(unique_stocks_dict.values())
min_stocks = min(unique_stocks_dict.values())
median_stocks = statistics.median(unique_stocks_dict.values())
f.write("The maximum number of unique stocks is " +
        str(max_stocks) + "." + "\n")
f.write("The minimum number of filings is " + str(min_stocks) + "." + "\n")
f.write("The median number of filings is " + str(median_stocks) + "." + "\n")
