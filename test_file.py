import pytest
from dotenv import load_dotenv
import os
import pymysql
import database


# Fixtures


@pytest.fixture
def setup():
    shaw_cik = '0001009268'
    shaw_date = '2005-08-25'
    shaw_holdings = [
        {
            "nameOfIssuer": "4D MOLECULAR THERAPEUTICS IN",
            "cusip": "35104E100",
            "titleOfClass": "COM",
            "value": 59541000,
            "shrsOrPrnAmt": {
                "sshPrnamt": 3937914,
                "sshPrnamtType": "SH"
            },
            "investmentDiscretion": "SOLE",
            "votingAuthority": {
                "Sole": 3937914,
                "Shared": 0,
                "None": 0
            }
        },
        {
            "nameOfIssuer": "ACADIA HEALTHCARE COMPANY IN",
            "cusip": "00404A109",
            "titleOfClass": "COM",
            "value": 139607000,
            "shrsOrPrnAmt": {
                "sshPrnamt": 2130433,
                "sshPrnamtType": "SH"
            },
            "investmentDiscretion": "SOLE",
            "votingAuthority": {
                "Sole": 2130433,
                "Shared": 0,
                "None": 0
            }
        },
        {
            "nameOfIssuer": "ADAPTIVE BIOTECHNOLOGIES COR",
            "cusip": "00650F109",
            "titleOfClass": "COM",
            "value": 416313000,
            "shrsOrPrnAmt": {
                "sshPrnamt": 29993708,
                "sshPrnamtType": "SH"
            },
            "investmentDiscretion": "SOLE",
            "votingAuthority": {
                "Sole": 29993708,
                "Shared": 0,
                "None": 0
            }
        }
    ]
    citadel_cik = '0001423053'
    citadel_date = '2020-10-05'
    citadel_holdings = [
        {
            "nameOfIssuer": "VANGUARD INDEX FDS",
            "cusip": "922908553",
            "titleOfClass": "REAL ESTATE ETF",
            "value": 32511000,
            "shrsOrPrnAmt": {
                "sshPrnamt": 300000,
                "sshPrnamtType": "SH"
            },
            "investmentDiscretion": "DFND",
            "votingAuthority": {
                "Sole": 0,
                "Shared": 300000,
                "None": 0
            },
            "putCall": "Put",
            "otherManager": "01,02"
        },
        {
            "nameOfIssuer": "WAYFAIR INC",
            "cusip": "94419LAB7",
            "titleOfClass": "NOTE 0.375% 9/0",
            "value": 2703000,
            "shrsOrPrnAmt": {
                "sshPrnamt": 2500000,
                "sshPrnamtType": "PRN"
            },
            "investmentDiscretion": "DFND",
            "votingAuthority": {
                "Sole": 0,
                "Shared": 2500000,
                "None": 0
            },
            "otherManager": "01,02"
        },
        {
            "nameOfIssuer": "WAYFAIR INC",
            "cusip": "94419LAD3",
            "titleOfClass": "NOTE 1.125%11/0",
            "value": 6102000,
            "shrsOrPrnAmt": {
                "sshPrnamt": 5000000,
                "sshPrnamtType": "PRN"
            },
            "investmentDiscretion": "DFND",
            "votingAuthority": {
                "Sole": 0,
                "Shared": 5000000,
                "None": 0
            },
            "otherManager": "01,02"
        }
    ]
    millenium_cik = '0001273087'
    millenium_date = '2022-03-31'
    millenium_holdings = [
        {
            "nameOfIssuer": "TESLA INC",
            "cusip": "88160RAG6",
            "titleOfClass": "NOTE 2.000% 5/1",
            "value": 43411000,
            "shrsOrPrnAmt": {
                "sshPrnamt": 2500000,
                "sshPrnamtType": "PRN"
            },
            "investmentDiscretion": "DFND",
            "votingAuthority": {
                "Sole": 0,
                "Shared": 2500000,
                "None": 0
            },
            "otherManager": "01,02"
        },
        {
            "nameOfIssuer": "TWITTER INC",
            "cusip": "90184L102",
            "titleOfClass": "COM",
            "value": 386900000,
            "shrsOrPrnAmt": {
                "sshPrnamt": 10000000,
                "sshPrnamtType": "SH"
            },
            "investmentDiscretion": "DFND",
            "votingAuthority": {
                "Sole": 0,
                "Shared": 10000000,
                "None": 0
            },
            "otherManager": "01,02"
        },
        {
            "nameOfIssuer": "TWITTER INC",
            "cusip": "90184LAN2",
            "titleOfClass": "NOTE 3/1",
            "value": 42346000,
            "shrsOrPrnAmt": {
                "sshPrnamt": 50000000,
                "sshPrnamtType": "PRN"
            },
            "investmentDiscretion": "DFND",
            "votingAuthority": {
                "Sole": 0,
                "Shared": 50000000,
                "None": 0
            },
            "otherManager": "01,02"
        },
        {
            "nameOfIssuer": "TWITTER INC",
            "cusip": "90184L102",
            "titleOfClass": "COM",
            "value": 77380000,
            "shrsOrPrnAmt": {
                "sshPrnamt": 2000000,
                "sshPrnamtType": "SH"
            },
            "investmentDiscretion": "DFND",
            "votingAuthority": {
                "Sole": 0,
                "Shared": 2000000,
                "None": 0
            },
            "putCall": "Call",
            "otherManager": "01,02"
        }
    ]
    return [shaw_cik, shaw_date, shaw_holdings, citadel_cik, citadel_date, citadel_holdings, millenium_cik, millenium_date, millenium_holdings]


@pytest.fixture
def cleanup():
    yield
    load_dotenv()
    connection = pymysql.connect(
        host=os.environ.get("DB_HOST"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        database=os.environ.get("TEST_DB_NAME")
    )
    # clean up database
    connection.cursor().execute("DELETE FROM `raw_13f_data`")
    connection.commit()

# Database tests


def test_add_raw_data(setup, cleanup):
    assert database.add_raw_13f_data_to_database(
        setup[0], setup[1], setup[2], True) == setup[0] + '-' + setup[1] + '-'
    assert database.add_raw_13f_data_to_database(
        setup[3], setup[4], setup[5], True) == setup[3] + '-' + setup[4] + '-'
    assert database.add_raw_13f_data_to_database(
        setup[6], setup[7], setup[8], True) == setup[6] + '-' + setup[7] + '-'
