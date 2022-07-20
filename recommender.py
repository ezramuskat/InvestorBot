import database
import pymysql
import os
import operator
from dotenv import load_dotenv

load_dotenv()

connection = pymysql.connect(
    host=os.environ.get("DB_HOST"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    database=os.environ.get("DB_NAME")
)


def get_stock_percentages_of_total_investment_per_quarter_ordered():
    """
    Returns a dictionary of quarters and the percentage of filings that have a given stock in them
    :return: A dictionary of tuples. The dictionary is keyed by quarter, and the tuples are (cusip,
    percentage)
    """
    cursor = connection.cursor()
    percentages = {}
    quarters = database.get_quarters()

    for quarter in quarters:
        cursor.execute(
            "SELECT cusip, (COUNT(DISTINCT cik)/14 * 100) AS PERCENTAGE FROM raw_13f_data WHERE quarter = %s AND put_call is NULL AND shareprn_type ='SH' AND NOT excluded GROUP BY cusip ORDER BY PERCENTAGE DESC;", (quarter))
        percentages[quarter] = cursor.fetchall()

    return percentages


def rank_percentages():
    percentages = get_stock_percentages_of_total_investment_per_quarter_ordered()

    # assign rankings
    rankings = {}
    total_count = database.get_count_of_total_unique_holdings()
    for quarter in percentages:
        scores = {}
        for i in range(len(quarter)):
            scores[quarter[i][0]] = total_count - i
        rankings[quarter] = scores

    # calculate
    final_ranking = []
    for stock in database.get_all_unique_holdings():
        scores = []
        for quarter in rankings:
            scores.append(quarter[stock])
        final_score = 0
        for score, weight in zip(scores, generate_weights(len(scores))):
            final_score += (score * weight)
        final_ranking.append((stock, final_score))

    final_ranking.sort(key=operator.itemgetter(1), reverse=True)

    return [ranking[0] for ranking in final_ranking]


def generate_weights(num):
    if num < 2:
        return [1]

    return_arr = []
    weight_count = 1
    for i in range(num, 0, -1):
        if i < 2:
            return_arr.extend([weight_count * (3/4), weight_count * (1/4)])
            break
        else:
            weight_count /= 2
            return_arr.append(weight_count)

    return return_arr
