"""
    This script implements methods that provide a set of comparinsons between search engines. It indexes a set of .txt files in Solr, Elasticsearch and OpenSearch.
    The goal is to use it to implement queries and benchmark each technology with a set of .txt files.
    Each file is equivalent to a new document indexed to a given collection/index, with all of the text being stored as the 'body' or 'content' field of the document.
"""

"""
    OBS.: Importante  ao utilizar uma instalação básica do ElasticSearch, verificar a versão. A partir da 8, parece que HTTPS é padrão e obrigatório, com necessidade de configurar questões de usuário e senha, certificados e uso de HTTPS ao invés de HTTP no caminho.
    A versão utilizada aqui é a 7.8.0 tanto para o servidor quanto para o pacote pypi elasticsearch, que está mais próxima da versão utilizada em produção atualmente no Querido Diário.
"""

import os
import pysolr
from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch
import logging
import json
import requests
import ssl
import urllib3
import certifi
import time
from typing import Dict
import xml.etree.ElementTree as ET

class SearchEngineIndexer:

    # Initialize logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    client = None

    # Global dictionary to store method runtimes
    time_records = {}

    # Disabling TSL warnings
    # urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Class constructor, starts according to a single search engine.
    def __init__(
        self, search_engine=None, hosts=None, index_name=None
    ):

        # Constants
        self.SEARCH_ENGINE_SL = "SOLR"
        self.SEARCH_ENGINE_ES = "ES"
        self.SEARCH_ENGINE_OS = "OS"

        # Set ES as default engine
        self.SEARCH_ENGINE_DEFAULT = self.SEARCH_ENGINE_ES

        # Select search engine or default
        self.search_engine = search_engine or self.SEARCH_ENGINE_DEFAULT
        # Define host address for the search service
        self.hosts = hosts
        # Define the name of the index to search
        self.index_name = index_name

        # Start the connection with the search engine, according to each client
        if search_engine == self.SEARCH_ENGINE_SL:
            # Perform indexing usingrequests instead of python client
            solr_host = hosts[0]
            if solr_host:
                self.client = pysolr.Solr(solr_host, timeout=10)
                # Health check:
                logging.info("*** SOLR - Status Health Check:")
                self.client.ping()
                # Set SOLR to use requests:
                # self.client = None
            # No PySOLR client available
            else:
                self.client = None
        elif search_engine == self.SEARCH_ENGINE_ES:
            try:
                # TODO: no necessity for login/pass on 7.8.0, but seems to be default in 8.8.0
                self.client = Elasticsearch(hosts, verify_certs=False)
                if self.client:
                    health = self.client.cluster.health()
                    logging.info("→→→ ES Client Health Status:")
                    logging.info(health)
                    # Test requests indexing, instead of python client
                    #self.client = None
                else:
                    logging.info("*** No ES Client available. ***")
            except Exception as e:
                self.logger.error(f"Error connecting to Elasticsearch: {e}")
        elif search_engine == self.SEARCH_ENGINE_OS:
            try:
                # To use the opensearch-py client
                self.client = OpenSearch(hosts, http_auth=('admin', 'admin'))
                # To use requests
                # self.client = None
                if self.client:
                    logging.info('Client successfully registered.')
                else:
                    logging.error('No OpenSearchS Client registered.')
            except Exception as e:
                self.logger.error("Error connecting to OpenSearch")
        else:
            self.logger.error("Invalid search engine. Nothing else to do.")

    # A method to assist keeping track of time_records for method executions
    def log_time_records(self, method_name, start_time, end_time):
        execution_time = end_time - start_time
        execution_time_str = f"{execution_time:.2f}s"
        self.time_records[method_name] = execution_time_str

    # A simple decorator prints the time spent in seconds to run the method
    def log_execution_time(method):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = method(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            execution_time_str = f"{execution_time:.2f}s"
            record = {'method': method.__name__, 'time': execution_time_str}
            logging.info(f"Execution time of {method.__name__}: {execution_time_str} seconds")
            return result
        return wrapper

    # This method opens a text file in memory returning its contents as part of a dictionary object with the file path as its id
    def process_file(self, file_path):
        # Extracts file name without path nor extension to use as file id
        doc_id = file_path[-44:-4]
        try:
            # Ensure directories are skipped
            if os.path.isfile(file_path):
                with open(file_path, "r") as file:
                    content = file.read()
                    body = {"id": doc_id, "content": content}
                    return body
        except Exception as e:
            logging.error(e)

    # Process and index all files in the data directory, client agnostic
    @log_execution_time
    def process_and_index_files(self, files_directory=None):
        processed_files_counter = 1
        if files_directory is None and self.files_directory is not None:
            for filename in os.listdir(self.files_directory):
                file_path = os.path.join(self.files_directory, filename)
                # Skip directories
                if ".txt" in file_path:
                    logging.info(f"*** Currently processing file nº: {processed_files_counter} [{file_path}]")
                    payload = self.process_file(file_path)
                    self.index_files(payload)
                    # Increment counter
                    processed_files_counter = processed_files_counter + 1
        else:
            if files_directory is not None:
                for filename in os.listdir(files_directory):
                    file_path = os.path.join(files_directory, filename)
                    if ".txt" in file_path:
                        logging.info(f"*** Currently processing file nº: {processed_files_counter} [{file_path}]")
                        payload = self.process_file(file_path)
                        self.index_files(payload)
                        # Increment counter
                        processed_files_counter = processed_files_counter + 1
            else:
                logging.error("No valid files directory available to process.")

    # Select the search engine to index the files based on the search_engine parameter and call the proper method to index data
    @log_execution_time
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

    # Write the file contents to the Solr index
    @log_execution_time
    def index_with_solr(self, payload):
        try:
            solr_client = self.client
            # Run indexing using solr client (pysolr or solrpy)
            if solr_client:
                logging.info("* Running indexing with SOLR client")
                logging.info(payload['id'])
                logging.info(payload['content'][:200])
                try:
                    # Add document item to index
                    response = solr_client.add([payload])
                    # Commit operation to solr index
                    solr_client.commit()
                except Exception as e:
                    logging.error("Indexing error in SOLR")
                    logging.error(e)
                # TODO retry SOLR python implementation using same solution as CKAN
            # Run indexing using requests
            else:
                logging.info("* Running indexing with SOLR via requests")
                # Convert the payload to JSON
                json_payload = json.dumps(payload)

                # Set the headers for the request
                headers = {"Content-type": "application/json"}

                url = f"{self.hosts[0]}/update/json/docs"
                # Send the index request to Solr
                response = requests.post(
                    url, data=json_payload, headers=headers, verify=certifi.where()
                )
                logging.info(f"Indexing SOLR with requests  via URL {url}")
                # Commit the changes to make them visible in the index
                requests.get(f"{self.hosts[0]}/update?commit=true")

                # Check the response status
                if response.status_code == 200:
                    logging.info("Document indexed successfully.")
                else:
                    logging.error("Failed to index the document.")
        except Exception as e:
            logging.error("Error indexing SOLR document.")
            logging.error(e)

    # Write the file contents to the Elasticsearch index. The method verifies if there is a python client setup for the search engine. If there is, it uses it. If there isn't it tries the requests approach.
    @log_execution_time
    def index_with_elasticsearch(self, payload):

        # Small fix in content to avoid some characters like \n•
        text_content = payload["content"]
        text_content = text_content.replace("\n•", "")
        payload["content"] = text_content

        short = payload["content"][:150]
        doc_id = payload["id"]
        #logging.info(f" *** [{doc_id}] Payload to be indexed: {short}")

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
            url = f"{base_url}/{index_name}/_doc/{doc_id}?refresh=true"
            headers = {"Content-Type": "application/json"}
            logging.info(f"→→→ Indexing with request calls to [{url}]")
            try:
                response = requests.put(
                    url, json=payload #, headers=headers #, verify=certifi.where()
                )

                if response:
                    logging.info(f"Response: {response}")
                    response_code = response.status_code
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
                else:
                    logging.error("→→→ Invalid response.")
            except Exception as e:
                logging.error(f'↓↓↓ Requests Indexing Error for ES: {e}')

    # Write the file contents to the OpenSearch index
    @log_execution_time
    def index_with_opensearch(self, payload):

        # Small fix in content to avoid some characters like \n•
        text_content = payload["content"]
        text_content = text_content.replace("\n•", "")
        payload["content"] = text_content

        short = payload["content"][:150]
        doc_id = payload["id"]
        #logging.info(f" *** [{doc_id}] Payload to be indexed: {short}")

        # In this case, use the Elasticsearch python client
        if self.client is not None:
            try:
                logging.info("→→→ Indexing with opensearch-py client")
                response = self.client.index(
                    self.index_name, id=doc_id, body=payload, refresh=True
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
        else:
            logging.info("→→→ Indexing with opensearch-py client")
            username = 'admin'
            password = 'admin'
            # Make sure to adjust the URL and payload as per your requirements
            url = f'http://localhost:9202/opensearch_index/_doc/{doc_id}?refresh=true'

            response = requests.put(url, json=payload, auth=(username, password))

            if response.status_code == 200:
                # Document indexed successfully
                logging.info('Document indexed successfully.')
            else:
                # Failed to index document
                logging.error(f'Failed to index document. Response: {response.content}')

    # Write a simple method to query an index for some keywords
    @log_execution_time
    def query_with_solr(self, query_str: str):
        # Search query
        query = f'content:"{query_str}"'

        solr_client = self.client
        # If there is a client, try to use it for the query
        if solr_client:
            logging.info("*** Querying with pySolr:")
            # Check search:
            results = solr_client.search(query)
            if results:
                logging.info(f"→→→ Results for query [{query}]:")
                logging.info(results)
                # Accessing fields
                logging.info(f"Total hits:{results.hits}")
                logging.info(f"Number of documents: {len(results)}")

                # Iterating over the documents
                for doc in results:
                    logging.info(f"ID: {doc['id']}")
            else:
                logging.error("→→→ No results for query.")
        # Using requests to RESTful API
        else:
            logging.info("Querying with requests:")
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
                content = result["content"][0][0:100]
                # Process or print the fields
                logging.info(f"Document ID: {doc_id}")
                logging.info(f"Content: {content}")

            # Print the total number of search results
            total_results = json_response["response"]["numFound"]
            print(f"Total results: {total_results}")

    @log_execution_time
    def query_with_elasticsearch(self, query_string):
        # set payload:
        # Search query
        search_query = {
            "query": {
                "match_phrase": {
                    "content": query_string
                }
            }
        }
        # TODO: Pick from parameters
        index_name = "elasticsearch_index"
        # Query ES using python client
        es = self.client
        if es:
            logging.info(f'→→→ Query {query_string} on ES Client')
            es_results = es.search(index=index_name, body=search_query)
            total = es_results['hits']['total']['value']
            logging.info(f"→→→ Resultados da consulta: [{total}]")
            for hit in es_results['hits']['hits']:
                text_content = hit['_source']['content']
                if text_content:
                    text_content = text_content[0:50]
                    logging.info(text_content)
        # Query ES using requests and RESTful API
        else:
            logging.info(f'→→→ Query {query_string} on ES using requests')
            # Elasticsearch server information
            base_url = "http://localhost:9200"

            # Search request
            url = f"{base_url}/{index_name}/_search"
            headers = {"Content-Type": "application/json"}
            payload = {"query": {"query_string": {"query": query_string}}}
            logging.info(f"Executando consulta ES: {payload}")
            response = requests.get(
                url, json=payload #, headers=headers, verify=certifi.where()
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

    @log_execution_time
    def query_with_opensearch(self, query_string, index_name):
        # Resgister start time
        start_time = time.time()

        # Search query
        search_query = {
            "query": {
                "match_phrase": {
                    "content": query_string
                }
            }
        }

        # Query OS using python client
        os = self.client
        if os:
            logging.info(f'→→→ Query [{query_string}] on OS Client')
            opensearch_results = os.search(index=index_name, body=search_query)
            logging.info("OpenSearch Results:")
            for hit in opensearch_results['hits']['hits']:
                content = hit['_source']['content']
                content = content[0:25]
                logging.info('→ RESULT CONTENT SAMPLE:')
                logging.info(content)

        # Query OS using requests and RESTful API
        else:
            logging.info(f'→→→ Query {query_string} on OS using requests')
            # Elasticsearch server information
            base_url = "http://localhost:9202"
            # Search request
            url = f"{base_url}/{index_name}/_search"
            payload = {"query": {"query_string": {"query": query_string}}}
            logging.info(f"Executando consulta ES: {payload}")
            response = requests.get(url, json=payload)
            if response:
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
            else:
                logging.error('* No response.')
        # Log time_records
        end_time = time.time()
        self.log_time_records('query_with_opensearch', start_time, end_time)

    # Run a query on solr with highlights
    @log_execution_time
    def highlight_solr(self, query_str: str):

        solr_client = self.client
        # If there is a client, try to use it for the query
        if solr_client:
            logging.info("*** Querying with pySolr:")
            # Search query
            query = f'content:"{query_str}"'
            params = {
                "hl": "true",                  # Enable highlighting
                "hl.fl": "content",                # Specify the field to highlight
                "hl.fragsize": 100,            # Fragment size (number of characters)
                "hl.snippets": 20,             # Number of snippets to return
                "hl.maxAnalyzedChars": 200000, # Maximum number of characters to analyze for highlighting
                "hl.simple.pre": "<strong>",   # Prefix for highlighted terms
                "hl.simple.post": "</strong>"  # Suffix for highlighted terms
            }
            # Check search:
            results = solr_client.search(query, **params)
            if results:
                logging.info(f"→→→ Results for query [{query}]:")
                logging.info(results)
                # Accessing fields
                logging.info(f"Total hits:{results.hits}")
                logging.info(f"Number of documents: {len(results)}")

                # Iterating over the documents
                document_ids = []

                for result in results:
                    logging.info(f"ID: {result['id']}")
                    document_ids.append(result['id'])
                # Highlights:
                highlighting = results.highlighting
                if highlighting:
                    logging.info("→→→→ Highlights →→→")
                    for doc_id in document_ids:
                        highlight = highlighting[doc_id]
                        logging.info(f"*** Highlight: {highlight['content']}")
    # Perform query using requests
        else:
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

    # Run a query on ElasticSearch with highlights
    @log_execution_time
    def highlight_elasticsearch(self, index_name: str, query_string: str):
        start_time = time.time()
        # set payload:
        # Search query
        match_phrase = {
            "match": {
                "content": query_string
            }
        }

        search_query = {
            "query": match_phrase,
            "highlight": {
                "fields": {
                    "content": {}
                },
                "fragment_size": 100,
                "number_of_fragments": 1,
                "pre_tags": [" →→→ "],
                "post_tags": [" ←←← "],
                "type": "unified"
            }
        }

        # Query ES using python client
        es = self.client
        if es:
            # Testing
            response = es.indices.get_mapping(index=index_name)
            logging.info(f"*** Mapping: {response}")

            logging.info(f'→→→ Query {query_string} on ES Client')
            es_results = es.search(index=index_name, body=search_query)
            total = es_results['hits']['total']['value']
            logging.info(f"→→→ Resultados da consulta: [{total}]")
            for hit in es_results['hits']['hits']:
                text_content = hit['_source']['content']
                if text_content:
                    text_content = text_content[0:50]
                    logging.info(text_content)
                # Access the highlighted content
                highlight = hit.get("highlight", {})
                highlighted_field = highlight.get("content", [])
                # Print the highlighted content
                logging.info("→ Highlights:")
                logging.info(highlighted_field)
        # Query ES using requests and RESTful API
        else:
            logging.info(f'→→→ Query {query_string} on ES using requests')
            # TODO: implement using requests
        end_time = time.time()
        self.log_time_records('highlight_elasticsearch', start_time, end_time)

    # Run a query on OpenSearch with highlights
    @log_execution_time
    def highlight_opensearch(self, query_str: str):
        # TODO: Adapt to Opensearch
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

        os_client = self.client
        # Use OS client to make queries:
        if os_client:
            logging.info('→ Highlight query with OpenSearch')
        # Use RESTFul requests to the API
        else:

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
    @log_execution_time
    def complex_query_highlight_solr(self, full_string, additional_terms):
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

    @log_execution_time
    def delete_solr(self, url):
        # Fetch client
        solr_client = self.client
        # Client deletion:
        if solr_client:
            query = '*:*'# Match documents with id iqual to anything, so all documents
            logging.info(f"* Deleting using SOLR client: {query}")
            try:
                response = solr_client.delete(q=query)
                solr_client.commit()
                logging.info(response)
                logging.info(f"Type of the response: {type(response)}")
                # Parse the XML response string
                root = ET.fromstring(response)

                # Find the status code element
                status_element = root.find(".//int[@name='status']")

                # Extract the status code
                status_code = int(status_element.text)
                if status_code == 0:
                    logging.info(f"→→→ Deletion of records from [{url}] successful")
                else:
                    logging.error("←←← No records deleted.")
            except Exception as e:
                logging.error("→ Error deleting in SOLR")
                logging.error(e)
        # Requests alternative
        else:
            logging.info("* Deleting SOLR records using Requests")
            # Send POST request to delete all documents
            delete_url = url + '/update/'

            delete_query = {
                "delete": { "query": "*:*" }
            }
            # Commit True is passed to ensure the operation is commited immediately
            query_params = {"commit": "true"}

            try:

                logging.info(f"url: {delete_url}")
                logging.info(f"query: {delete_query}")
                response = requests.post(delete_url, json=delete_query, params=query_params)
                # Extract the status code from requests response
                status_code = int(response.status_code)
                if status_code == 200:
                    logging.info(f"→→→ Deletion of records from [{url}] successful")
                else:
                    logging.error("←←← No records deleted.")
                    return 0
            except Exception as e:
                logging.error("Error on the Requests delete for SOLR")
                logging.error(e)

    @log_execution_time
    def delete_elasticsearch(self, index_name):
        url = f"http://localhost:9200/{index_name}/_delete_by_query"

        # Set request headers
        headers = {"Content-Type": "application/json"}

        # Set the query payload
        query = {"query": {"match_all": {}}}  # Match all documents

        es = self.client
        # Client deletion:
        if es:
            logging.info("* Deleting using ES client.")
            response = es.delete_by_query(
                index=index_name,
                body=query
            )
            # Check if the delete operation was successful
            n_deleted = response["deleted"]
            if n_deleted:
                logging.info(f"→→→ {n_deleted} documents deleted successfully.")
            else:
                logging.error("No documents deleted.")
        # Requests alternative
        else:
            logging.info("* Deleting using requests")
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

    # Auxiliary method to set a given elasticsearch index with a given config set.
    def set_pt_br_analyzer_elasticsearch(self, url: str, index_name: str):
        analyzer_config = {
            "settings": {
                "analysis": {
                    "analyzer": {
                        "brazilian_with_stopwords": {
                            "tokenizer": "standard",
                            "filter": ["lowercase", "brazilian_stemmer"],
                        },
                        "exact": {
                            "tokenizer": "standard",
                            "filter": ["lowercase"],
                        }
                    }
                }
            }
            # TODO: Apply config to the index using index or requests
        }

        # Create the index with the Brazilian analyzer configuration
        response = requests.put(f"{url}/{index_name}", json=analyzer_config)
        if response.status_code == 200:
            logging.info(f"Successfully updated the index '{index_name}' with the Brazilian analyzer.")
        else:
            logging.error(f"Failed to update the index '{index_name}'.")

    def close_connections(self):
        exit_client = self.client
        if exit_client:
            exit_client.close()
