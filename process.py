import logging
import time
import requests
import os
import pandas as pd
from urllib.request import urlopen
import certifi
import ssl
import json
from dv_utils import default_settings, Client
import delta_sharing



logger = logging.getLogger(__name__)

# let the log go to stdout, as it will be captured by the cage operator
logging.basicConfig(
    level=default_settings.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def get_jsonparsed_data(url):
    """
    Receive the content of ``url``, parse it as JSON and return the object.

    Parameters
    ----------
    url : str

    Returns
    -------
    dict
    """
    response = urlopen(url)
    data = response.read().decode("utf-8")
    return json.loads(data)

def load_parquet(
        profile_file_full_path: str,
        shared_table_url: str
    ):

        # Point to the profile file. It can be a file on the local file system or a file on a remote storage.
        profile_file = os.path.dirname(__file__) + profile_file_full_path
        table_url = profile_file_full_path + shared_table_url
        logger.info(f"Point to the profile file. at {table_url}")
        data = delta_sharing.load_as_pandas(table_url)
        return data

def calculate_score(
        df,
        apiKey: str,
        totalJson
    ):
        index_tickers = df['ticker_symbol'].tolist() #assigning all tickers to a list
        for index, row in df.iterrows():
            try:
                qte=row['quantity']
                ticker=row['ticker_symbol']
                url = (f"https://financialmodelingprep.com/api/v4/esg-environmental-social-governance-data?symbol={ticker}&apikey={apiKey}")
                esg_json=get_jsonparsed_data(url)
                totalJson['totalQte']=totalJson['totalQte']+int(qte)
                environmentalScore=esg_json[0]['environmentalScore']
                totalJson['totalEnvironmentalScore']=totalJson['totalEnvironmentalScore']+int(qte)*float(environmentalScore)
                socialScore=esg_json[0]['socialScore']
                totalJson['totalSocialScore']=totalJson['totalSocialScore']+int(qte)*float(socialScore)
                governanceScore=esg_json[0]['governanceScore']
                totalJson['totalGovernanceScore']=totalJson['totalGovernanceScore']+int(qte)*float(governanceScore)
                ESGScore=esg_json[0]['ESGScore']
                totalJson['totalESGScore']=totalJson['totalESGScore']+int(qte)*float(ESGScore)
            except IndexError:
                logger.info(f"Error getting financial data for ticker: {ticker}")
        return totalJson


# define an event processing function
def event_processor(evt: dict):
    """
    Process an incoming event
    """
    start = time.time()

    try:
        logger.info(f"Processing event {evt}")

        evt_type =evt.get("type", "")
        if(evt_type == "CALCULATE_ESG_SCORE"):
           logger.info(f"use the calculate_esg_score event processor")
           update_quote_event_processor_delta_share(evt)

    # pylint: disable=broad-except
    except Exception as err:
        logger.error(f"Failed processing event: {err}")
    finally:
        logger.info(f"Processed event in {time.time() - start:.{3}f}s")


def update_quote_event_processor(evt: dict):
    client = Client()
    API_KEY="586e328526b6e172fe4a25e02c6624b8"
    #API_KEY=os.environ.get("API_KEY", "")
    PORTFOLIO_CSV_ENTITY_BE_PATH=os.path.dirname(__file__) + "/portfolios_entity-be.csv"
    df = pd.read_csv(PORTFOLIO_CSV_ENTITY_BE_PATH) #give the full path of file downloaded
    index_tickers = df['ticker_symbol'].tolist() #assigning all tickers to a list
    totalEnvironmentalScore=0
    totalSocialScore=0
    totalGovernanceScore=0
    totalESGScore=0
    totalQte=0
    for index, row in df.iterrows():
       qte=row['quantity']
       ticker=row['ticker_symbol']
       url = (f"https://financialmodelingprep.com/api/v4/esg-environmental-social-governance-data?symbol={ticker}&apikey={API_KEY}")
       esg_json=get_jsonparsed_data(url)
       totalQte=totalQte+qte
       environmentalScore=esg_json[0]['environmentalScore']
       totalEnvironmentalScore=totalEnvironmentalScore+qte*environmentalScore
       socialScore=esg_json[0]['socialScore']
       totalSocialScore=totalSocialScore+qte*socialScore
       governanceScore=esg_json[0]['governanceScore']
       totalGovernanceScore=totalGovernanceScore+qte*governanceScore
       ESGScore=esg_json[0]['ESGScore']
       totalESGScore=totalESGScore+qte*ESGScore
    #fileLocation=os.environ.get("ESG_TOTAL_SCORE_PATH", "")
    fileLocation="./esg-total-score.json"
    logger.info(f"Write anonymized output file at {fileLocation}")
    totalJson={'totalESGScore':totalESGScore/totalQte,'totalEnvironmentalScore':totalEnvironmentalScore/totalQte,'totalSocialScore':totalSocialScore/totalQte,'totalGovernanceScore':totalGovernanceScore/totalQte}
    with open(fileLocation, 'w', newline='') as file:
        file.write(json.dumps(totalJson))



def update_quote_event_processor_delta_share(evt: dict):
   client = Client()
   API_KEY=os.environ.get("API_KEY", "")
   #API_KEY="586e328526b6e172fe4a25e02c6624b8"

   DELTA_SHARE_PROFILE_PATH = os.environ.get("DELTA_SHARE_PROFILE_PATH", "")
   #DELTA_SHARE_PROFILE_PATH="./open-datasets.share"
   if(not DELTA_SHARE_PROFILE_PATH):
       raise RuntimeError("DELTA_SHARE_PROFILE_PATH environment variable is not defined")
   
   DELTA_SHARE_TABLE_ENTITY_BE_URL = os.environ.get("DELTA_SHARE_TABLE_ENTITY_BE_URL", "")
   #DELTA_SHARE_TABLE_ENTITY_BE_URL="#delta-portfolios-entity-be.default.delta-portfolios-entity-be-dataset"
   if(not DELTA_SHARE_TABLE_ENTITY_BE_URL):
       raise RuntimeError("DELTA_SHARE_TABLE_ENTITY_BE_URL environment variable is not defined")

   DELTA_SHARE_TABLE_ENTITY_LU_URL = os.environ.get("DELTA_SHARE_TABLE_ENTITY_LU_URL", "")
   #DELTA_SHARE_TABLE_ENTITY_LU_URL="#delta-portfolios-entity-lu.default.delta-portfolios-entity-lu-dataset"
   if(not DELTA_SHARE_TABLE_ENTITY_LU_URL):
       raise RuntimeError("DELTA_SHARE_TABLE_ENTITY_LU_URL environment variable is not defined")
   
   totalJson={'totalQte':0,'totalESGScore':0,'totalEnvironmentalScore':0,'totalSocialScore':0,'totalGovernanceScore':0}
   
   #calculate for ENTITY_BE customers
   df=load_parquet(DELTA_SHARE_PROFILE_PATH,DELTA_SHARE_TABLE_ENTITY_BE_URL)  
   totalJson=calculate_score(df,API_KEY,totalJson)

   #calculate for ENTITY_LU customers
   df=load_parquet(DELTA_SHARE_PROFILE_PATH,DELTA_SHARE_TABLE_ENTITY_LU_URL)  
   totalJson=calculate_score(df,API_KEY,totalJson)
   
   fileLocation=os.environ.get("ESG_TOTAL_SCORE_PATH", "")
   #fileLocation="./esg-total-score.json"
   logger.info(f"Write anonymized output file at {fileLocation}")
   averageJson={'totalESGScore':totalJson['totalESGScore']/totalJson['totalQte'],'totalEnvironmentalScore':totalJson['totalEnvironmentalScore']/totalJson['totalQte'],'totalSocialScore':totalJson['totalSocialScore']/totalJson['totalQte'],'totalGovernanceScore':totalJson['totalGovernanceScore']/totalJson['totalQte']}
   with open(fileLocation, 'w', newline='') as file:
       file.write(json.dumps(averageJson))


