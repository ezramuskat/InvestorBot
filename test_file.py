from dotenv import load_dotenv
import pymysql
import database
import evalute
import operator
from datetime import datetime
import recommender


def generate_per_quarter_rankings():
    """
    It takes the output of the two conviction methods, ranks them, and averages the rankings to get a
    final ranking
    :return: A dictionary of the top 20 stocks for each quarter.
    """
    percentages = recommender.get_consensus_stock_percentages_per_quarter_ordered()
    all_stocks = database.get_all_unique_holdings()
    rankings = {}
    for i, quarter in enumerate(percentages.keys()):
        print(quarter)
        final_ranking = {}
        consensus_scoring = generate_ranking_for_single_quarter(
            percentages[quarter], len(all_stocks))
        # Since this line is borderline unreadable, this takes the dictionary output from the scored but unranked conviction methods, ranks it, and gives it back in a nice,  useable list
        conviction_scoring = {stock: idx for idx, stock in enumerate(generate_ranking_for_single_quarter(
            list(evalute.rank1(evalute.amountinvest(i, i)).items()), len(all_stocks)))}
        # print(consensus_scoring)
       # print(conviction_scoring)

        if len(consensus_scoring) != len(conviction_scoring):
            raise Exception(
                f"lists of ranking are not the same length; consensus has {len(consensus_scoring)} elements, while conviction has {len(conviction_scoring)}")
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
        rankings[quarter] = database.get_stock_names_from_cusips(
            return_list[:20])
    return rankings


def generate_ranking_for_single_quarter(scoring, length):
    """
    Takes in a list of stocks in a relevant order and a length, and returns a list of stocks sorted by their score

    :param scoring: a dictionary of the form {'stock_symbol': score}
    :param length: the number of total stocks (holdover so we can reuse some code from recommender)
    :return: A list of stocks in order of their score.
    """
    scores = recommender.generate_scores_for_quarter(
        scoring, length)

    final_ranking = []
    for stock in scores:
        final_ranking.append((stock, scores[stock]))

    final_ranking.sort(key=operator.itemgetter(1), reverse=True)

    return [ranking[0] for ranking in final_ranking]


def index_comparison(index, stock, other_list):
    if stock in other_list:
        return abs(index - other_list.index(stock))
    else:
        return None


if __name__ == "__main__":
    file_name = "model_tests/test-" + \
        datetime.now().strftime("%d-%m-%Y-%H:%M:%S") + ".txt"
    f = open(file_name, "w")
    # for each quarter, starting from the third one in the database
    quarters = database.get_quarters()
    actual_results = generate_per_quarter_rankings()
    for index, quarter in enumerate(quarters[2:-1], 2):
        print(quarter)
        f.write("Predictions for " + quarter + "\n")
        # get the recommendations for that quarter
        recommendations = recommender.recommend_stocks(index)
        # get the recommendations based solely on the next quarter's 13f data
        actual = actual_results[quarter]
        # calculate some similarity metrics
        num_overlapping_stocks = len(set(recommendations) & set(actual))
        deviations_from_position = list(filter(None, [index_comparison(
            idx, stock, recommendations) for idx, stock in enumerate(actual)]))

        # put the two into a text file for comparison
        f.write("\tPredictions: " + str(recommendations) + "\n")
        f.write("\tActual: " + str(actual) + "\n")
        f.write("\tNumber of stock correctly picked: " +
                str(num_overlapping_stocks) + "\n")
        f.write("\tAverage stock deviation from correct position: " +
                str(sum(deviations_from_position)/len(deviations_from_position)) + "\n")
