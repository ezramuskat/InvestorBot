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
f.write("The minimum number of unique stocks is " +
        str(min_stocks) + "." + "\n")
f.write("The median number of unique stocks is " +
        str(median_stocks) + "." + "\n")

# Get overall and average holdings per year + standard deviation

cursor.execute(
    "SELECT cik, AVG(value), quarter FROM raw_13f_data GROUP BY cik, quarter ORDER BY cik DESC")

average_stocks = cursor.fetchall()

values = []

for average in average_stocks:
    f.write(" " + str(average[0]) + " had an average of " +
            str(average[1]) + " invested on " + str(average[2]) + "\n")
    values.append(average[1])

max_average = max(values)
min_average = min(values)
median_average = statistics.median(values)
f.write("The maximum number of average stocks is " +
        str(max_average) + "." + "\n")
f.write("The minimum number of average stocks is " +
        str(min_average) + "." + "\n")
f.write("The median number of average stocks is " +
        str(median_average) + "." + "\n")

# Get portfolio percentages

f.write("Portfolio data: " + "\n")

cursor.execute(
    "SELECT DISTINCT quarter FROM raw_13f_data ORDER BY quarter DESC")

quarters = []

for quarter in cursor.fetchall():
    quarters.append(quarter[0])

for quarter in quarters:
    f.write("\tPortfolios for " + quarter + ": " + "\n")
    cursor.execute(
        "SELECT cik, cusip, value FROM raw_13f_data WHERE quarter = %s ORDER BY cik DESC", [quarter])
    cik_dict = {}
    percentages = []
    for holding in cursor.fetchall():
        if holding[0] not in cik_dict:
            cik_dict[holding[0]] = {}
        cik_dict[holding[0]][holding[1]] = holding[2]
    for cik in cik_dict:
        f.write("\t \t " + str(cik) + ": " + "\n")
        holdings = cik_dict[cik]
        total_value = sum(holdings.values())
        for holding in holdings:
            percent_value = holdings[holding] / total_value * 100
            percentages.append(percent_value)
            f.write("\t \t \t" + str(holding) + ": " +
                    str(percent_value) + "%\n")
    max_percentage = max(percentages)
    min_percentage = min(percentages)
    median_percentage = statistics.median(percentages)
    f.write("\t The maximum percentage for this quarter is " +
            str(max_percentage) + "." + "\n")
    f.write("\t The minimum percentage for this quarter is " +
            str(min_percentage) + "." + "\n")
    f.write("\t The median percentage for this quarter is " +
            str(median_percentage) + "." + "\n")
