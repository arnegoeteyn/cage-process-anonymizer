import logging
import time
import requests
import os
import pandas as pd
from dv_utils import default_settings, Client
import delta_sharing

import csv
import pprint
import collections
from typing import List, Iterable, Optional, Union, Dict

from presidio_analyzer import AnalyzerEngine, BatchAnalyzerEngine, RecognizerResult, DictAnalyzerResult
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import EngineResult

"""
Example implementing a CSV analyzer

This example shows how to use the Presidio Analyzer and Anonymizer
to detect and anonymize PII in a CSV file.
It uses the BatchAnalyzerEngine to analyze the CSV file, and the
BatchAnonymizerEngine to anonymize the requested columns.
Note that currently BatchAnonymizerEngine is not part of the anonymizer package,
and is defined in this and the batch_processing notebook.
https://github.com/microsoft/presidio/blob/main/docs/samples/python/batch_processing.ipynb

Content of csv file:
id,name,city,comments
1,John,New York,called him yesterday to confirm he requested to call back in 2 days
2,Jill,Los Angeles,accepted the offer license number AC432223
3,Jack,Chicago,need to call him at phone number 212-555-5555

"""

logger = logging.getLogger(__name__)

# let the log go to stdout, as it will be captured by the cage operator
logging.basicConfig(
    level=default_settings.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

class CSVAnalyzer(BatchAnalyzerEngine):

    def analyze_csv(
        self,
        csv_full_path: str,
        language: str,
        keys_to_skip: Optional[List[str]] = None,
        **kwargs,
    ) -> Iterable[DictAnalyzerResult]:

        with open(csv_full_path, 'r') as csv_file:
            csv_list = list(csv.reader(csv_file))
            csv_dict = {header: list(map(str, values)) for header, *values in zip(*csv_list)}
            analyzer_results = self.analyze_dict(csv_dict, language, keys_to_skip)
            return list(analyzer_results)

class ParquetAnalyzer(BatchAnalyzerEngine):
    def analyze_parquet(
        self,
        profile_file_full_path: str,
        shared_table_url: str,
        language: str,
        keys_to_skip: Optional[List[str]] = None,
        **kwargs,
    ) -> Iterable[DictAnalyzerResult]:

        # Point to the profile file. It can be a file on the local file system or a file on a remote storage.
        profile_file = profile_file_full_path
        table_url = profile_file_full_path + shared_table_url
        data = delta_sharing.load_as_pandas(table_url)
        frame_list = data.values.tolist()
        frame_dict = {header: list(map(str, values)) for header, *values in zip(*frame_list)}
        analyzer_results = self.analyze_dict(frame_dict, language, keys_to_skip)
        return list(analyzer_results)
        

class BatchAnonymizerEngine(AnonymizerEngine):
    """
    Class inheriting from the AnonymizerEngine and adding additional functionality 
    for anonymizing lists or dictionaries.
    """

    def anonymize_list(
        self,
        texts:List[Union[str, bool, int, float]], 
        recognizer_results_list: List[List[RecognizerResult]], 
        **kwargs
    ) -> List[EngineResult]:
        """
        Anonymize a list of strings.

        :param texts: List containing the texts to be anonymized (original texts)
        :param recognizer_results_list: A list of lists of RecognizerResult,
        the output of the AnalyzerEngine on each text in the list.
        :param kwargs: Additional kwargs for the `AnonymizerEngine.anonymize` method
        """
        return_list = []
        if not recognizer_results_list:
            recognizer_results_list = [[] for _ in range(len(texts))]
        for text, recognizer_results in zip(texts, recognizer_results_list):
            if type(text) in (str, bool, int, float):
                res = self.anonymize(text=str(text), analyzer_results=recognizer_results, **kwargs)
                return_list.append(res.text)
            else:
                return_list.append(text)

        return return_list

    def anonymize_dict(self, analyzer_results: Iterable[DictAnalyzerResult], **kwargs) -> Dict[str, str]:

        """
        Anonymize values in a dictionary.

        :param analyzer_results: Iterator of `DictAnalyzerResult` 
        containing the output of the AnalyzerEngine.analyze_dict on the input text.
        :param kwargs: Additional kwargs for the `AnonymizerEngine.anonymize` method
        """

        return_dict = {}
        for result in analyzer_results:

            if isinstance(result.value, dict):
                resp = self.anonymize_dict(analyzer_results = result.recognizer_results, **kwargs)
                return_dict[result.key] = resp

            elif isinstance(result.value, str):
                resp = self.anonymize(text=result.value, analyzer_results=result.recognizer_results, **kwargs)
                return_dict[result.key] = resp.text

            elif isinstance(result.value, collections.abc.Iterable):
                anonymize_respones = self.anonymize_list(texts=result.value,
                                                         recognizer_results_list=result.recognizer_results, 
                                                         **kwargs)
                return_dict[result.key] = anonymize_respones 
            else:
                return_dict[result.key] = result.value
        return return_dict

# define an event processing function
def event_processor(evt: dict):
    """
    Process an incoming event
    """
    start = time.time()

    try:
        logger.info(f"Processing event {evt}")

        evt_type =evt.get("type", "")
        if(evt_type == "ANONYMISE"):
           logger.info(f"use the anonymise event processor")
           update_quote_event_processor_delta_share(evt)

    # pylint: disable=broad-except
    except Exception as err:
        logger.error(f"Failed processing event: {err}")
    finally:
        logger.info(f"Processed event in {time.time() - start:.{3}f}s")


def update_quote_event_processor(evt: dict):
   client = Client()

   PII_CSV_PATH = os.environ.get("PII_CSV_PATH", "")
   if(not PII_CSV_PATH):
       raise RuntimeError("PII_CSV_PATH environment variable is not defined")
   
   analyzer = CSVAnalyzer()
   analyzer_results = analyzer.analyze_csv(PII_CSV_PATH,
                                            language="en")

   anonymizer = BatchAnonymizerEngine()
   anonymized_results = anonymizer.anonymize_dict(analyzer_results)
   fileLocation=os.environ.get("PII_CSV_ANONYMIZED_PATH", "")
   logger.info(f"Write anonymized output file at {fileLocation}")
   with open(os.environ.get("PII_CSV_ANONYMIZED_PATH", ""), 'w', newline='') as file:
        for key in anonymized_results.keys():
            file.write("%s,%s\n"%(key,anonymized_results[key]))

def update_quote_event_processor_delta_share(evt: dict):
   client = Client()

   DELTA_SHARE_PROFILE_PATH = os.environ.get("DELTA_SHARE_PROFILE_PATH", "")
   #DELTA_SHARE_PROFILE_PATH=os.path.dirname(__file__) + "/open-datasets.share"
   if(not DELTA_SHARE_PROFILE_PATH):
       raise RuntimeError("DELTA_SHARE_PROFILE_PATH environment variable is not defined")

   DELTA_SHARE_TABLE_URL = os.environ.get("DELTA_SHARE_TABLE_URL", "")
   #DELTA_SHARE_TABLE_URL= "#pii-share.default.pii-dataset"
   if(not DELTA_SHARE_TABLE_URL):
       raise RuntimeError("DELTA_SHARE_TABLE_URL environment variable is not defined")
   
   analyzer = ParquetAnalyzer()
   analyzer_results = analyzer.analyze_parquet(DELTA_SHARE_PROFILE_PATH,DELTA_SHARE_TABLE_URL,
                                            language="en")

   anonymizer = BatchAnonymizerEngine()
   anonymized_results = anonymizer.anonymize_dict(analyzer_results)
   fileLocation=os.environ.get("PII_CSV_ANONYMIZED_PATH", "")
   #fileLocation="./pii_file_anonymised.csv"
   logger.info(f"Write anonymized output file at {fileLocation}")
   with open(fileLocation, 'w', newline='') as file:
        for key in anonymized_results.keys():
            file.write("%s,%s\n"%(key,anonymized_results[key]))
