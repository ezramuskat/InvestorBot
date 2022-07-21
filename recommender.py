import database
import pymysql
import os
import operator
import sys
import itertools
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
        quarters = quarters[:num_quarters+1]

    for quarter in quarters:
        cursor.execute(
            "SELECT cusip, (COUNT(DISTINCT cik)/14 * 100) AS PERCENTAGE FROM raw_13f_data WHERE quarter = %s AND put_call is NULL AND shareprn_type ='SH' AND NOT excluded GROUP BY cusip ORDER BY PERCENTAGE DESC;", (quarter))
        percentages[quarter] = cursor.fetchall()

    return percentages


def rank_percentages(num_quarters=sys.maxsize):
    """
    Takes the consensus stock percentages per quarter, assigns a score to each stock based on its
    ranking in each quarter, and then calculates a final score for each stock based on the scores from
    each quarter
    :return: A list of stocks in order of their final score.
    """
    percentages = get_consensus_stock_percentages_per_quarter_ordered(
        num_quarters)

    # assign rankings
    rankings = {}
    for quarter in percentages:
        rankings[quarter] = generate_scores_for_quarter(percentages[quarter])

    # calculate
    final_ranking = []
    for stock in database.get_all_unique_holdings():
        scores = []
        for quarter in rankings:
            try:
                scores.append(rankings[quarter][stock])
            except KeyError:
                # stock is not present in this particular quarter
                scores.append(0)

        final_score = 0
        for score, weight in zip(scores, generate_weights(len(scores))):
            final_score += (score * weight)
        final_ranking.append((stock, final_score))

    final_ranking.sort(key=operator.itemgetter(1), reverse=True)

    return [ranking[0] for ranking in final_ranking]


def generate_scores_for_quarter(quarter_data):
    """
    For each holding in the quarter, assign it a score based on its rank in the quarter

    :param quarter_data: a list of tuples, where each tuple is a holding and its count
    :return: A dictionary of scores for each holding.
    """
    scores = {}
    total_count = database.get_count_of_total_unique_holdings()
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
        if i < 2:
            return_arr.extend([weight_count * (3/4), weight_count * (1/4)])
            break
        else:
            weight_count /= 2
            return_arr.append(weight_count)

    return return_arr


def recommend_stocks(num_quarters=sys.maxsize):
    consensus_scoring = rank_percentages(num_quarters)
    conviction_scoring = []  # placeholder

    if len(consensus_scoring) != len(conviction_scoring):
        raise Exception(
            f"lists of ranking are not the same length; consensus has {len(consensus_scoring)} elements, while conviction has {len(conviction_scoring)}")

    conviction_scoring_dict = {stock: index for index,
                               stock in enumerate(conviction_scoring)}

    final_ranking = [None] * len(consensus_scoring)
    for index, stock in enumerate(consensus_scoring):
        ranking_index = (index + conviction_scoring_dict[stock]) // 2
        if final_ranking[ranking_index] is None:
            final_ranking[ranking_index] = [stock]
        else:
            final_ranking[ranking_index].append(stock)

    return list(itertools.chain(*final_ranking))[:20]
