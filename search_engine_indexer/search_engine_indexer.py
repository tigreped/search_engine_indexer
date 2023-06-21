"""
    This script implements a comparinson between search engines. It indexes a set of .txt files in Solr, Elasticsearch and OpenSearch.
    Tee goal is to use it to implement queries and benchmark each technology with a set of 100 .txt files.
    Each file is equivalent to a new document indexed, with all of the text being stored as the 'body' field of the document.
"""

"""
    OBS.: Importante, ao utilizar uma instalação básica do ElasticSearch, cuidado para não confundir e passar https ao invés de http no endereço do serviço ES. Se não estiver configurado o certificado corretamente, vai
        acarretar em problemas e erros de TLS.
"""

import os
from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch
import logging
import json
import requests
import ssl
import urllib3
import certifi


class SearchEngineIndexer:

    # Initialize logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    client = None
    # Disabling TSL warnings
    # urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def __init__(
        self, files_directory=None, search_engine=None, hosts=None, index_name=None
    ):

        # Constants
        self.SEARCH_ENGINE_SL = "SOLR"
        self.SEARCH_ENGINE_ES = "ES"
        self.SEARCH_ENGINE_OS = "OS"

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

        # Start the connection with the search engine, accordingly
        if search_engine == self.SEARCH_ENGINE_SL:
            logging.info("SOLR has no client available, only RESTful API requests.")
        elif search_engine == self.SEARCH_ENGINE_ES:
            try:
                # TODO: no necessity for login/pass on 7.8.0, but seems to be default in 8.8.0
                self.client = Elasticsearch(hosts, verify_certs=False)
                if self.client:
                    health = self.client.cluster.health()
                    logging.info("→→→ ES Client Health Status:")
                    logging.info(health)
                    # Test requests indexing
                    #self.client = None
                else:
                    logging.info("*** No ES Client available. ***")
            except Exception as e:
                self.logger.error(f"Error connecting to Elasticsearch: {e}")
        elif search_engine == self.SEARCH_ENGINE_OS:
            try:
                self.client = OpenSearch(hosts)
            except Exception as e:
                self.logger.error("Error connecting to OpenSearch")
        else:
            self.logger.error("Invalid search engine. Nothing else to do.")

    # This method opens a text file in memory returning its contents as part of a dictionary object with the file path as its id
    def process_file(self, file_path):
        #TODO: extract file names without path to use as file id
        doc_id = file_path[-44:-4]
        logging.info(f'→→→→→→ id: {doc_id}')
        try:
            # Ensure directories are skipped
            if os.path.isfile(file_path):
                with open(file_path, "r") as file:
                    content = file.read()
                    body = {"id": doc_id, "content": content}
                    return body
        except Exception as e:
            logging.error(e)

    # Select the search engine to index the files based on the search_engine parameter and call the proper method to index data
    def index_files(self, payload):
        try:
            if self.search_engine == self.SEARCH_ENGINE_SL:
                self.index_with_solr(payload)
            elif self.search_engine == self.SEARCH_ENGINE_ES:
                self.index_with_elasticsearch(payload)
            elif self.search_engine == self.SEARCH_ENGINE_OS:
                self.index_with_opensearch(payload)
            else:
                self.logger.error("Invalid search engine")
        except Exception as e:
            logging.error('Something went wrong with indexing.')

    # Write the file contents to the Elasticsearch index
    def index_with_elasticsearch(self, payload):

        # Fix the content to avoid some characters like •
        text_content = payload["content"]
        text_content = text_content.replace("•", "")
        payload["content"] = text_content

        short = payload["content"][:150]
        doc_id = payload["id"]
        logging.info(f" *** [{doc_id}] Payload to be indexed: {short}")

        # In this case, use the Elasticsearch python client
        if self.client is not None:
            try:
                # logging.info(self.client.info())
                response = self.client.index(
                    index=self.index_name, body=payload, refresh=True
                )
                if response:
                    response_code = response["result"]
                    # Check response is 201 for upload or insert PUT requests
                    if response_code == "created":
                        logging.info("*** Document indexed successfully.")
                    else:
                        logging.error(
                            f"*** Failed to index document. Response: {response}"
                        )
                else:
                    logging.error("Something went wrong.")
            except Exception as e:
                logging.error(e)
        # In this case, use the requests to access the ES RESTful server
        else:
            # if payload:
            logging.info("→→→ Indexing with the request method:")
            # a Elasticsearch server information
            base_url = "http://localhost:9200"
            index_name = "elasticsearch_index"

            # Indexing request
            url = f"{base_url}/{index_name}/_doc"
            headers = {"Content-Type": "application/json"}
            logging.info(f"Indexing with request calls to [{url}]")
            response = requests.put(
                url, json=payload, headers=headers, verify=certifi.where()
            )

            if response:
                response_code = response["status_code"]
                if response_code:
                    # Check response
                    if response_code == 201:
                        logging.info(
                            f"*** Document id [{doc_id}] indexed successfully."
                        )
                    else:
                        logging.error(
                            f"*** Failed to index document id [{doc_id}]. Response: {response.content.decode()}"
                        )
                else:
                    logging.error("→→→ Something went wrong.")

    # Write the file contents to the OpenSearch index
    def index_with_opensearch(self, payload):
        self.client.index(index=self.index_name, body=payload)

    # Write the file contents to the Solr index
    def index_with_solr(self, payload):
        try:

            # Convert the payload to JSON
            json_payload = json.dumps(payload)

            # Set the headers for the request
            headers = {"Content-type": "application/json"}

            url = f"{self.hosts[0]}/update/json/docs"
            # Send the index request to Solr
            response = requests.post(
                url, data=json_payload, headers=headers, verify=certifi.where()
            )
            logging.info(f"Trying to request SOLR via URL {url}")
            # Commit the changes to make them visible in the index
            requests.get(f"{self.hosts[0]}/update?commit=true")

            # Check the response status
            if response.status_code == 200:
                logging.info("Document indexed successfully.")
            else:
                logging.error("Failed to index the document.")
        except Exception as e:
            logging.error("Erro de adição de documento ao SOLR.")
            logging.error(e)

    # Write a simple method to query an index for some keywords
    def query_with_solr(self, query_str: str):
        # Search query
        query = f'content:"{query_str}"'
        # Query parameters
        params = {"q": query}
        # Send the search request to Solr
        response = requests.get(f"{self.hosts[0]}/select", params=params)

        # Parse the response JSON
        json_response = response.json()

        # Get the search results
        results = json_response["response"]["docs"]

        # Process the search results
        for result in results:
            # Access the document fields
            doc_id = result["id"]
            # Limit the amount of characters to display
            content = result["content"][0][0:200]
            # Process or print the fields
            logging.info(f"Document ID: {doc_id}")
            logging.info(f"Content: {content}")

        # Print the total number of search results
        total_results = json_response["response"]["numFound"]
        print(f"Total results: {total_results}")

    def highlight_solr(self, query_str: str):
        # Search query
        query = f'content:"{query_str}"'
        # Query parameters
        params = {
            "q": query,
            "hl": "true",  # Enable highlighting
            "hl.fl": "content",  # Specify the field to highlight
            "hl.fragsize": 250,
            "hl.snippets": 20,
            "hl.maxAnalyzedChars": 200000,
            "hl.simple.pre": "<strong>",  # Prefix for highlighted terms
            "hl.simple.post": "</strong>",  # Suffix for highlighted terms
        }

        # Send the search request to Solr
        response = requests.get(f"{self.hosts[0]}/select", params=params)

        # Parse the response JSON
        json_response = response.json()

        # Get the search results
        results = json_response["response"]["docs"]

        # Process the search results
        for result in results:
            # Access the document fields
            doc_id = result["id"]
            content = result["content"][0][0:200]

            # Access the highlight information
            highlights = json_response["highlighting"][doc_id]["content"]

            # Process or print the fields and highlights
            logging.info(f"Document ID: {doc_id}")
            logging.info(f"**** HIGHLIGHTS: {len(highlights)}")
            logging.info(highlights)
            # Loop through the highlights and print up to 10 highlights
            for i, highlight in enumerate(highlights[:10]):
                logging.info(f"\n →→→ Highlight {i+1}: \n{highlight}\n")

        # Print the total number of search results
        total_results = json_response["response"]["numFound"]
        logging.info(f" * Total results: {total_results}")

    # Method to make a complex query with highlighting in SOLR
    def complex_query_highlight(self, full_string, additional_terms):
        # Define the logical operators ('AND' and 'OR')
        logical_operator_and = "AND"
        logical_operator_or = "OR"

        if additional_terms is not None:
            if len(additional_terms) > 0:
                query_string = f'"{full_string}" {logical_operator_and} ("'
                query_string += (
                    f'" {logical_operator_or} "'.join(additional_terms) + '")'
                )
            else:
                query_string = f'"{full_string}"'

        logging.info(f"→→→ [query_string]: {query_string}")

        # Set the parameters for the Solr query
        params = {
            "q": query_string,
            "hl": "true",  # Enable highlighting
            "hl.q": query_string,
            "hl.fl": "content",  # Specify the field to highlight
            "hl.fragsize": 250,
            "hl.snippets": 20,
            "hl.requireFieldMatch" "hl.maxAnalyzedChars": 200000,
            "hl.usePhraseHighlighter": "true",
            "hl.useFastVectorHighlighter": "true",
            "hl.method": "fastVector",
            "hl.simple.pre": "<strong>",  # Prefix for highlighted terms
            "hl.simple.post": "</strong>",  # Suffix for highlighted terms
        }

        # Send the Solr query request
        response = requests.get(f"{self.hosts[0]}/select", params=params)

        # Parse the response JSON
        data = response.json()

        # Get the highlighted content from the response
        highlights = data["highlighting"]

        # Loop through the highlights and print them
        for doc_id, doc_highlights in highlights.items():
            logging.info(f"Document ID: {doc_id}")
            for field, field_highlights in doc_highlights.items():
                logging.info(f"Field: {field}")
                for highlight in field_highlights:
                    logging.info(f"Highlight: {highlight}")

    def query_elasticsearch(self, query_string):
        # Elasticsearch server information
        base_url = "http://localhost:9200"
        index_name = "elasticsearch_index"

        # Search request
        url = f"{base_url}/{index_name}/_search"
        headers = {"Content-Type": "application/json"}
        payload = {"query": {"query_string": {"query": query_string}}}
        logging.info(f"Executando consulta ES: {payload}")
        response = requests.get(
            url, json=payload, headers=headers, verify=certifi.where()
        )

        # Check response
        if response.status_code == 200:
            results = response.json()
            hits = results.get("hits", {}).get("hits", [])
            total_hits = results.get("hits", {}).get("total", {}).get("value", 0)
            logging.info(f"→→→ Total results: {total_hits}")

            # Process each hit
            for hit in hits:
                source = hit.get("_source", {})
                logging.info(source)  # Print the document source
            return hits

        else:
            print(f"Failed to perform search. Response: {response.content.decode()}")

    def delete_elasticsearch(self, index_name):
        url = f"http://localhost:9200/{index_name}/_delete_by_query"

        # Set request headers
        headers = {"Content-Type": "application/json"}

        # Set the query payload
        query = {"query": {"match_all": {}}}  # Match all documents

        # Send POST request to delete all documents
        response = requests.post(url, json=query, headers=headers)

        # Check response status code
        if response.status_code == 200:
            # Parse the response JSON
            data = response.json()
            # Get the number of deleted documents
            deleted_count = data.get("deleted", 0)
            logging.info(f"Deleted {deleted_count} documents from index '{index_name}'")
            return deleted_count
        else:
            logging.error(f"Error: {response.content}")
            return 0

    # Process and index all files in the data directory, client agnostic
    def process_and_index_files(self, files_directory=None):
        processed_files_counter = 1
        if files_directory is None and self.files_directory is not None:
            for filename in os.listdir(self.files_directory):
                file_path = os.path.join(self.files_directory, filename)
                logging.info(f"*** Currently processing file nº: {processed_files_counter} [{file_path}]")
                payload = self.process_file(file_path)
                self.index_files(payload)
                # Increment counter
                processed_files_counter = processed_files_counter + 1
        else:
            if files_directory is not None:
                for filename in os.listdir(files_directory):
                    file_path = os.path.join(files_directory, filename)
                    logging.info(f"*** Currently processing file nº: {processed_files_counter} [{file_path}]")
                    payload = self.process_file(file_path)
                    self.index_files(payload)
                    # Increment counter
                    processed_files_counter = processed_files_counter + 1
            else:
                logging.error("No valid files directory available to process.")

    def close_connections(self):
        exit_client = self.client
        if exit_client:
            exit_client.close()
