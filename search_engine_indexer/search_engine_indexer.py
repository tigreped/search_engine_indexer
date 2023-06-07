'''
    This script implements a comparinson between search engines. It indexes a set of .txt files in Solr, Elasticsearch and OpenSearch.
    Tee goal is to use it to implement queries and benchmark each technology with a set of 100 .txt files.
    Each file is equivalent to a new document indexed, with all of the text being stored as the 'body' field of the document.
'''

import os
from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch
import logging
import json
import requests

class SearchEngineIndexer:

    # Initialize logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    client = None

    def __init__(self, files_directory=None, search_engine=None, hosts=None, index_name=None):

        # Constants
        self.SEARCH_ENGINE_SL = 'SOLR'
        self.SEARCH_ENGINE_ES = 'ES'
        self.SEARCH_ENGINE_OS = 'OS'

        # Set ES as default engine
        self.SEARCH_ENGINE_DEFAULT = self.SEARCH_ENGINE_ES
        # Directory with the txt files
        self.files_directory = files_directory
        # Select search engine or default
        self.search_engine = search_engine or self.SEARCH_ENGINE_DEFAULT
        # Define host address for the search service
        self.hosts = hosts
        # Define the name of the index to search
        self.index_name = index_name
        # Start the connection with the search engine accordingly
        if search_engine == self.SEARCH_ENGINE_SL:
            print("SOLR has no client available, only RESTful API requests.")
        elif search_engine == self.SEARCH_ENGINE_ES:
            try:
                self.client = Elasticsearch(hosts, basic_auth=['elastic', 'S1nsJ8nYcz76sYC15p7f'], verify_certs=False)
            except Exception as e:
                self.logger.error("Error connecting to Elasticsearch")
                print(e)
        elif search_engine == self.SEARCH_ENGINE_OS:
            try:
                self.client = OpenSearch(hosts)
            except Exception as e:
                self.logger.error("Error connecting to OpenSearch")
                print(e)
        else:
            self.logger.error("Invalid search engine. Nothing else to do.")

    # This method opens a text file in memory returning its contents as part of a dictionary object with the file path as its id
    def process_file(self, file_path):
        # Ensure directories are skipped
        if os.path.isfile(file_path):
            with open(file_path, 'r') as file:
                content = file.read()
                body = {
                    'id': file_path,
                    'content': content
                }
                return body

    # Select the search engine to index the files based on the search_engine parameter and call the proper method to index data
    def index_files(self, payload):
        if self.search_engine == self.SEARCH_ENGINE_SL:
            self.index_with_solr(payload)
        elif self.search_engine == self.SEARCH_ENGINE_ES:
            self.index_with_elasticsearch(payload)
        elif self.search_engine == self.SEARCH_ENGINE_OS:
            self.index_with_opensearch(payload)
        else:
            self.logger.error("Invalid search engine")

    # Write the file contents to the Elasticsearch index
    def index_with_elasticsearch(self, payload):
        if self.client is not None:
            self.client.index(index=self.index_name, body=payload)
        else:
            print('Must retry connectino to the server and index')

    # Write the file contents to the OpenSearch index
    def index_with_opensearch(self, payload):
        self.client.index(index=self.index_name, body=payload)

    # Write the file contents to the Solr index
    def index_with_solr(self, payload):
        try:

            # Convert the payload to JSON
            json_payload = json.dumps(payload)

            # Set the headers for the request
            headers = {
                'Content-type': 'application/json'
            }

            url = f'{self.hosts[0]}/update/json/docs'
            # Send the index request to Solr
            response = requests.post(url, data=json_payload, headers=headers)
            logging.info(f"Trying to request SOLR via URL {url}")
            # Commit the changes to make them visible in the index
            requests.get(f'{self.hosts[0]}/update?commit=true')

            # Check the response status
            if response.status_code == 200:
                logging.info('Document indexed successfully.')
            else:
                logging.error('Failed to index the document.')
        except Exception as e:
            logging.error("Erro de adição de documento ao SOLR.")
            logging.error(e)

    # Write a simple method to query an index for some keywords
    def query_with_solr(self, query_str:str):
        # Search query
        query = f'content:"{query_str}"'
        # Query parameters
        params = {
            'q': query
        }
        # Send the search request to Solr
        response = requests.get(f'{self.hosts[0]}/select', params=params)

        # Parse the response JSON
        json_response = response.json()

        # Get the search results
        results = json_response['response']['docs']

        # Process the search results
        for result in results:
            # Access the document fields
            doc_id = result['id']
            # Limit the amount of characters to display
            content = result['content'][0][0:200]
            # Process or print the fields
            logging.info(f"Document ID: {doc_id}")
            logging.info(f"Content: {content}")

        # Print the total number of search results
        total_results = json_response['response']['numFound']
        print(f'Total results: {total_results}')

    def highlight_solr(self, query_str:str):
        # Search query
        query = f'content:"{query_str}"'
        # Query parameters
        params = {
            'q': query,
            'hl': 'true',  # Enable highlighting
            'hl.fl': 'content',  # Specify the field to highlight
            'hl.fragsize': 250,
            'hl.snippets': 20,
            'hl.maxAnalyzedChars': 200000,
            'hl.simple.pre': '<strong>',  # Prefix for highlighted terms
            'hl.simple.post': '</strong>'  # Suffix for highlighted terms
        }

        # Send the search request to Solr
        response = requests.get(f'{self.hosts[0]}/select', params=params)

        # Parse the response JSON
        json_response = response.json()

        # Get the search results
        results = json_response['response']['docs']

        # Process the search results
        for result in results:
            # Access the document fields
            doc_id = result['id']
            content = result['content'][0][0:200]

            # Access the highlight information
            highlights = json_response['highlighting'][doc_id]['content']


            # Process or print the fields and highlights
            logging.info(f"Document ID: {doc_id}")
            logging.info(f"**** HIGHLIGHTS: {len(highlights)}")
            logging.info(highlights)
            # Loop through the highlights and print up to 10 highlights
            for i, highlight in enumerate(highlights[:10]):
               logging.info(f"\n →→→ Highlight {i+1}: \n{highlight}\n")

        # Print the total number of search results
        total_results = json_response['response']['numFound']
        logging.info(f' * Total results: {total_results}')

    # Process and index all files in the data directory
    def process_and_index_files(self, files_directory=None):
        if files_directory is None and self.files_directory is not None:
            for filename in os.listdir(self.files_directory):
                file_path = os.path.join(self.files_directory, filename)
                payload = self.process_file(file_path)
                self.index_files(payload)
        else:
            if files_directory is not None:
                for filename in os.listdir(files_directory):
                    file_path = os.path.join(files_directory, filename)
                    payload = self.process_file(file_path)
                    self.index_files(payload)
            else:
                logging.error("No valid files directory available to process.")
    def close_connections(self):
        self.client.close()
