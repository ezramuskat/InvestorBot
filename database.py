from dotenv import load_dotenv
import os
import pymysql


def add_raw_13f_data_to_database(cik, quarter, holdings):
    """
    Takes in a cik, quarter, and list of holdings, and adds them to the database

    :param cik: The CIK of the company that the 13F is for
    :param quarter: The quarter of the filing
    :param holdings: a list of dictionaries, each dictionary representing a holding
    :return: The starting sequence for the IDs of all entries added by this method.
    """
    # load .env data, create the format for the beginning of the id, and set up the format for the queries
    load_dotenv()
    id_start = str(cik) + '-' + str(quarter) + '-'
    sql = "INSERT INTO `raw_13f_data` (`HoldingID`, `cik`, `quarter`, `issuer`, `cusip`, `class`, `value`, `shareprn_amount`, `shareprn_type`, `put_call`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    # connect to the database
    connection = pymysql.connect(
        host=os.environ.get("DB_HOST"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        database=os.environ.get("DB_NAME")
    )
    # go through each individual holding
    for holding in holdings:
        # add the holding to the database
        put_call = holding.get("putCall", None)
        holding_id = id_start + holding["cusip"]
        if put_call != None:
            holding_id = holding_id + '-' + put_call

        connection.cursor().execute(sql, (holding_id, cik, quarter, holding["nameOfIssuer"], holding["cusip"], holding[
            "titleOfClass"], holding["value"], holding["shrsOrPrnAmt"]["sshPrnamt"], holding["shrsOrPrnAmt"]["sshPrnamtType"], put_call))
        connection.commit()
    # Returning the id_start variable.
    return id_start
