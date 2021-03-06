import time
import database
import pymysql
import os
import operator
import sys
import itertools
import evalute
from dotenv import load_dotenv

load_dotenv()

connection = pymysql.connect(
    host=os.environ.get("DB_HOST"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    database=os.environ.get("DB_NAME")
)


def get_consensus_stock_percentages_per_quarter_ordered(num_quarters=sys.maxsize):
    """
    Returns a dictionary of quarters and the percentage of filings that have a given stock in them
    :return: A dictionary of tuples. The dictionary is keyed by quarter, and the tuples are (cusip,
    percentage)
    """
    cursor = connection.cursor()
    percentages = {}
    quarters = database.get_quarters()

    if len(quarters) > num_quarters:
        quarters = quarters[:num_quarters]

    for quarter in quarters:
        cursor.execute(
            "SELECT cusip, (COUNT(DISTINCT cik)/14 * 100) AS PERCENTAGE FROM raw_13f_data WHERE quarter = %s AND put_call is NULL AND shareprn_type ='SH' AND NOT excluded GROUP BY cusip ORDER BY PERCENTAGE DESC;", (quarter))
        percentages[quarter] = cursor.fetchall()

    return percentages


def rank_percentages(percentages):
    """
    Takes the consensus stock percentages per quarter, assigns a score to each stock based on its
    ranking in each quarter, and then calculates a final score for each stock based on the scores from
    each quarter
    :return: A list of stocks in order of their final score.
    """

    # assign rankings
    rankings = {}
    all_stocks = database.get_all_unique_holdings()
    for quarter in percentages:
        rankings[quarter] = generate_scores_for_quarter(
            percentages[quarter], len(all_stocks))

    # calculate
    final_ranking = []
    weights = generate_weights(len(rankings))
    for stock in all_stocks:
        scores = []
        for quarter in rankings:
            try:
                scores.append(rankings[quarter][stock])
            except KeyError:
                # stock is not present in this particular quarter
                scores.append(0)

        final_score = 0
        for score, weight in zip(scores, weights):
            final_score += (score * weight)
        final_ranking.append((stock, final_score))

    final_ranking.sort(key=operator.itemgetter(1), reverse=True)

    return [ranking[0] for ranking in final_ranking]


def generate_scores_for_quarter(quarter_data, total_count):
    """
    For each holding in the quarter, assign it a score based on its rank in the quarter

    :param quarter_data: a list of tuples, where each tuple is a holding and its count
    :return: A dictionary of scores for each holding.
    """
    scores = {}
    for i in range(len(quarter_data)):
        scores[quarter_data[i][0]] = total_count - i
    return scores


def generate_weights(num):
    """
    Takes the number of weights you want to generate, and returns a list of weights that sum to 1

    :param num: the number of weights to generate
    :return: a list of weights that are used to calculate the weighted average of the data.
    """
    if num < 2:
        return [1]

    return_arr = []
    weight_count = 1
    for i in range(num, 0, -1):
        if i < 3:
            return_arr = [weight_count *
                          (0.25), weight_count * (0.75)] + return_arr
            break
        else:
            weight = weight_count * 0.5
            return_arr.insert(0, weight)
            weight_count -= weight

    return return_arr


def recommend_stocks(num_quarters=sys.maxsize):
    """
    Takes the average of the consensus and conviction rankings, and returns the top 20 stocks

    :param num_quarters: the number of quarters to look at
    :return: A list of the top 20 stocks to buy
    """
    # print(time.perf_counter())
    consensus_scoring = rank_percentages(
        get_consensus_stock_percentages_per_quarter_ordered(num_quarters))
    # print(time.perf_counter())
    conviction_scoring = evalute.finle_eval(num_quarters)
    # print(time.perf_counter())
    if len(consensus_scoring) != len(conviction_scoring):
        raise Exception(
            f"lists of ranking are not the same length; consensus has {len(consensus_scoring)} elements, while conviction has {len(conviction_scoring)}")

    final_ranking = {}
    for index, stock in enumerate(consensus_scoring):
        ranking_index = (index + conviction_scoring[stock]) // 2
        if ranking_index not in final_ranking:
            final_ranking[ranking_index] = [stock]
        else:
            final_ranking[ranking_index].append(stock)

    # print(sorted(final_ranking.keys()))
    return_list = []
    for ranking_index in sorted(final_ranking.keys()):
        return_list.extend(final_ranking[ranking_index])
    # print(time.perf_counter())
    return database.get_stock_names_from_cusips(return_list[:20])
