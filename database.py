from dotenv import load_dotenv
import os
import pymysql


def add_raw_13f_data_to_database(cik, quarter, data):
    """
    Takes in a cik, quarter, and a set of holdings, and adds the data to the database

    :param cik: The CIK of the company that the 13F is for
    :param quarter: The quarter of the 13F filing
    :param data: The holdings to be added to the database
    :return: The start of the id for all the holdings added.
    """
    # load .env data, create the format for the beginning of the id, and set up the format for the queries
    load_dotenv()
    id_start = str(cik) + '-' + str(quarter) + '-'
    sql = "INSERT INTO `raw_13f_data ` (`id`, `cik`, `quarter`, `issuer`, `cusip`, `class`, `value`, `share/prn_amt`, `share/prn_type`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    # connect to the database
    connection = pymysql.connect(
        host=os.environ.get("DB_HOST"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        database=os.environ.get("DB_NAME")
    )
    # go through each individual holding
    for i, holding in enumerate(data):
        # add the holding to the databas
        holding_id = id_start + str(i)
        connection.cursor().execute(sql, (holding_id, cik, quarter, holding["nameOfIssuer"], holding["cusip"], holding[
            "titleOfClass"], holding["value"], holding["shrsOrPrnAmt"]["sshPrnamt"], holding["shrsOrPrnAmt"]["sshPrnamtType"]))
        connection.commit()
    # Returning the id_start variable.
    return id_start
