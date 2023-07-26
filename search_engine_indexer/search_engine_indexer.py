"""
    This script implements methods that provide a set of comparinsons between search engines. It indexes a set of .txt files in Solr, Elasticsearch and OpenSearch.
    The goal is to use it to implement queries and benchmark each technology with a set of .txt files.
    Each file is equivalent to a new document indexed to a given collection/index, with all of the text being stored as the 'body' or 'content' field of the document.
"""

"""
    OBS.: Importante  ao utilizar uma instalação básica do ElasticSearch, verificar a versão. A partir da 8, parece que HTTPS é padrão e obrigatório, com necessidade de configurar questões de usuário e senha, certificados e uso de HTTPS ao invés de HTTP no caminho.
    A versão utilizada aqui é a 7.8.0 tanto para o servidor quanto para o pacote pypi elasticsearch, que está mais próxima da versão utilizada em produção atualmente no Querido Diário.
"""
from typing import List
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
from typing import Dict, List
import xml.etree.ElementTree as ET
from querido_diario_toolbox.process.text_process import remove_breaks


#OBS.: Para o ElasticSearch, houve algum problema de memória ou disco que gera um watermark e faz com que o índice fique apenas em modo de leitura. Para contornar fiz:
# curl -XPUT -H "Content-Type: application/json" http://localhost:9200/_all/_settings -d '{"index.blocks.read_only_allow_delete": null}'
# curl -XPUT -H "Content-Type: application/json" http://localhost:9200/_cluster/settings -d '{ "transient": { "cluster.routing.allocation.disk.threshold_enabled": false } }'


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
    def __init__(self, search_engine=None, hosts=None, index_name=None):

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
                # Obs: no necessity for login/pass on 7.8.0, but seems to be default in 8.8.0, behaves as OpenSearch
                self.client = Elasticsearch(hosts, verify_certs=False, timeout=30)
                if self.client:
                    logging.info("*** ElasticSearch client available. ***")
                    self.get_server_status_elasticsearch()

                    # Uncoment in order to test Usage of Requests instead of python client
                    # self.client = None
                else:
                    logging.info("*** No ES Client available. ***")
            except Exception as e:
                self.logger.error(f"Error connecting to Elasticsearch: {e}")
        elif search_engine == self.SEARCH_ENGINE_OS:
            try:
                # To use the opensearch-py client
                self.client = OpenSearch(hosts, http_auth=("admin", "admin"))
                # To use requests
                # self.client = None
                if self.client:
                    logging.info("Client successfully registered.")
                else:
                    logging.error("No OpenSearchS Client registered.")
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
            record = {"method": method.__name__, "time": execution_time_str}
            logging.info(
                f"Execution time of {method.__name__}: {execution_time_str} seconds"
            )
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
                    logging.info(
                        f"*** Currently processing file nº: {processed_files_counter} [{file_path}]"
                    )
                    payload = self.process_file(file_path)
                    self.index_files(payload)
                    # Increment counter
                    processed_files_counter = processed_files_counter + 1
        else:
            if files_directory is not None:
                for filename in os.listdir(files_directory):
                    file_path = os.path.join(files_directory, filename)
                    if ".txt" in file_path:
                        logging.info(
                            f"*** Currently processing file nº: {processed_files_counter} [{file_path}]"
                        )
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
            logging.error("Something went wrong with indexing.")

    # Write the file contents to the Solr index
    # Client: Ok | Requests: Ok
    @log_execution_time
    def index_with_solr(self, payload):
        try:
            solr_client = self.client
            clean_content = remove_breaks(payload["content"])
            # Save default english text field
            payload["content_en"] = clean_content
            # Index using brazillian portuguese analyzer
            payload["content_br"] = clean_content
            payload["content"] = None
            logging.info(clean_content[:200])

            # Run indexing using solr client (pysolr or solrpy)
            if solr_client:
                logging.info("* Running indexing with SOLR client")
                logging.info(payload["id"])
                # Clean content
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
                json_payload = json.dumps(payload, indent=3)

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
    # Client: Ok | Requests: Ok
    @log_execution_time
    def index_with_elasticsearch(self, payload):
        start_time = time.time()

        # Small fix in content to avoid some characters like \n•
        text_content = payload["content"]
        text_content = text_content.replace("\n•", "")
        #
        payload["content_en"] = text_content
        payload["content_br"] = text_content
        payload["content"] = None
        doc_id = payload["id"]

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
                    url, json=payload  # , headers=headers #, verify=certifi.where()
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
                logging.error(f"↓↓↓ Requests Indexing Error for ES: {e}")
        end_time = time.time()
        self.log_time_records("index_with_elasticsearch", start_time, end_time)

    # Write the file contents to the OpenSearch index
    # Client: Ok | Requests: Ok
    @log_execution_time
    def index_with_opensearch(self, payload):

        # Small fix in content to avoid some characters like \n•
        text_content = payload["content"]
        text_content = text_content.replace("\n•", "")
        # Indexes text content according to different language analyzers and delete default "content" field from dict
        payload["content_en"] = text_content
        payload["content_br"] = text_content
        payload["content"] = None
        doc_id = payload["id"]

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
            logging.info("→→→ Indexing with Requests")
            username = "admin"
            password = "admin"
            # Make sure to adjust the URL and payload as per your requirements
            url = f"http://localhost:9202/opensearch_index/_doc/{doc_id}?refresh=true"

            response = requests.put(url, json=payload, auth=(username, password))

            if response.status_code == 200:
                # Document indexed successfully
                logging.info("Document indexed successfully.")
            else:
                # Failed to index document
                logging.error(f"Failed to index document. Response: {response.content}")

    # Write a simple method to query an index for some keywords
    # Client: Ok | Requests: Ok
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

                logging.info(f"→→→ Search for phrase [{query_str}]:")

                # Iterating over the documents
                for doc in results:
                    logging.info(f"ID: {doc['id']}")
                    # String content
                    content = doc["content"][0]
                    # Remove breaks using querido_diario_toolbox
                    content = remove_breaks(content)
                    # Provides entire content string and recovers size characters of the content, keeping the phrase in the center of it.
                    size = 200
                    content = self.get_centered_fragment(content, size, query_str)
                    logging.info(f"→→→ Content: \n{content}")
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

    # Write a method to query the index for some keywords
    # Client: Ok | Requests: Ok
    @log_execution_time
    def query_with_elasticsearch(self, query_string):
        # set payload:
        # Search query
        search_query = {"query": {"match_phrase": {"content": query_string}}}
        # TODO: Pick from parameters
        index_name = "elasticsearch_index"
        # Query ES using python client
        es = self.client
        if es:
            logging.info(f"→→→ Query {query_string} on ES Client")
            response = es.search(index=index_name, body=search_query)
            total = response["hits"]["total"]["value"]
            logging.info(f"→→→ Resultados da consulta: [{total}]")
            for hit in response["hits"]["hits"]:
                text_content = hit["_source"]["content"]
                if text_content:
                    text_content = text_content[0:50]
                    logging.info(text_content)
        # Query ES using requests and RESTful API
        else:
            logging.info(f"→→→ Query {query_string} on ES using requests")
            # Elasticsearch server information
            base_url = "http://localhost:9200"

            # Search request
            url = f"{base_url}/{index_name}/_search"
            headers = {"Content-Type": "application/json"}
            payload = {"query": {"query_string": {"query": query_string}}}
            logging.info(f"Executando consulta ES: {payload}")
            response = requests.get(
                url, json=payload  # , headers=headers, verify=certifi.where()
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
                print(
                    f"Failed to perform search. Response: {response.content.decode()}"
                )

    # Write a method to query the index for some keywords
    # Client: Ok | Requests: Ok
    @log_execution_time
    def query_with_opensearch(self, query_string, index_name):
        # Resgister start time
        start_time = time.time()

        # Search query
        search_query = {"query": {"match_phrase": {"content": query_string}}}

        # Query OS using python client
        os = self.client
        if os:
            logging.info(f"→→→ Query [{query_string}] on OS Client")
            opensearch_results = os.search(index=index_name, body=search_query)
            logging.info("OpenSearch Results:")
            for hit in opensearch_results["hits"]["hits"]:
                content = hit["_source"]["content"]
                content = content[0:25]
                logging.info("→ RESULT CONTENT SAMPLE:")
                logging.info(content)

        # Query OS using requests and RESTful API
        else:
            logging.info(f"→→→ Query {query_string} on OS using requests")
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
                    total_hits = (
                        results.get("hits", {}).get("total", {}).get("value", 0)
                    )
                    logging.info(f"→→→ Total results: {total_hits}")

                    # Process each hit
                    for hit in hits:
                        source = hit.get("_source", {})
                        logging.info(source)  # Print the document source
                    return hits

                else:
                    print(
                        f"Failed to perform search. Response: {response.content.decode()}"
                    )
            else:
                logging.error("* No response.")
        # Log time_records
        end_time = time.time()
        self.log_time_records("query_with_opensearch", start_time, end_time)

    # Run a query on solr with highlights
    # Client: Ok | Requests: Ok
    @log_execution_time
    def highlight_solr(self, query_str: str, field_name: str):
        start_time = time.time()

        solr_client = self.client
        # If there is a client, try to use it for the query
        if solr_client:
            logging.info("*** Querying with pySolr:")
            # Search query
            query = f'{field_name}:"{query_str}"'
            not_query = f'-content_br:("denúncia 180")'
            # query = query + ' ' + not_query
            logging.info(f"→→→→→→→→→→→→→→→ query: {query}")

            type = "unified" # fastVector, original
            # Set the field to search in
            params = {
                "df": field_name,
                "hl": "true",  # Enable highlighting
                'hl.q': query,
                "hl.fl": field_name,  # Specify the field to highlight
                "hl.fragsize": 100,  # Fragment size (number of characters)
                "hl.snippets": 20,  # Number of snippets to return
                "hl.maxAnalyzedChars": 200000,  # Maximum number of characters to analyze for highlighting
                "hl.simple.pre": "→→→",  # Prefix for highlighted terms
                "hl.simple.post": "←←←",  # Suffix for highlighted terms
                "hl.method": type # Highlighter type
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
                    document_ids.append(result["id"])
                # Highlights:
                highlighting = results.highlighting
                if highlighting:
                    logging.info("→→→→ Highlights →→→")
                    for doc_id in document_ids:
                        highlight = highlighting[doc_id]
                        logging.info(f"*** Highlight: {highlight[field_name]}")
        # Perform query using requests
        else:
            # Search query
            query = f'{field_name}:"{query_str}"'
            # Query parameters
            params = {
                "q": query,
                "hl": "true",  # Enable highlighting
                "hl.q": query,
                "hl.fl": field_name,  # Specify the field to highlight
                "hl.fragsize": 100,
                "hl.snippets": 10,
                "hl.maxAnalyzedChars": 200000,
                "hl.simple.pre": "→→→",  # Prefix for highlighted terms
                "hl.simple.post": "←←←",  # Suffix for highlighted terms
                "hl.method": type
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
                content = result[field_name][0][0:200]

                # Access the highlight information
                highlights = json_response["highlighting"][doc_id][field_name]

                # Process or print the fields and highlights
                logging.info(f"Document ID: {doc_id}")
                logging.info(f"**** HIGHLIGHTS: {len(highlights)}")
                # Loop through the highlights and print up to 10 highlights
                for i, highlight in enumerate(highlights[:10]):
                    logging.info(f"\n →→→ Highlight {i+1}: \n{highlight}\n")

            # Print the total number of search results
            total_results = json_response["response"]["numFound"]
            logging.info(f" * Total results: {total_results}")
        # Log time_records
        end_time = time.time()
        self.log_time_records("highlight_solr", start_time, end_time)

    # Run a query on ElasticSearch with highlights
    # field_name is the field that the query will be searched in
    # Client: Ok | Requests: TODO
    @log_execution_time
    def highlight_elasticsearch(self, url: str, index_name: str, query_string: str, exclude_terms: List, field_name: str, proximity_distance=None):
        start_time = time.time()



        # set payload:
        # Search query
        # highlighter type:
        type = "fvh"  # unified, plain, fvh
        query_body = {
            "query": {
                "bool": {
                    "filter": [],
                    "must": {
                        "match_phrase": {
                            field_name: {
                                "query": query_string,
                                "slop": proximity_distance or 0
                            }
                        }
                    }
                }
            },
            "highlight": {
                "fields": {
                    field_name: {
                        "matched_fields": [field_name]
                    }
                },
                "type": type,
                "pre_tags": "→→→",
                "post_tags": "←←←",
                "number_of_fragments": 20,
                "fragment_size": 400,
                "order": "score"
            }
        }

        # Setup must_not mixin to exclude black list terms from query
        # Tried applying only to the highlight_query with no success
        if exclude_terms:
            # Parse the terms into blocks of match clauses for each term
            terms_str = " ".join(exclude_terms)
            query_body["query"]["bool"]["must_not"] = {
                "match_phrase": {
                    field_name: {
                        "query": terms_str,
                        "slop": proximity_distance or 0
                    }
                }
            }
        # Query ES using python client
        es = self.client
        if es:
            # response = es.search(index=index_name, body=query_body)
            # Build the query body with match_phrase and highlight settings
            logging.info("*** Using python client")
            # Execute the search query
            response = es.search(index=index_name, body=query_body)

            # Process the search results
            if "hits" in response:
                hits = response["hits"]["hits"]
                for hit in hits:
                    highlight = hit.get("highlight", {})
                    highlighted_field = highlight.get(field_name, [])

                    # Print the highlighted content
                    logging.info("→→→ Highlights:")
                    for i, highlight in enumerate(highlighted_field[:10]):
                        logging.info(f"\n →→→ Highlight {i+1}: \n{highlight}\n")
            else:
                logging.info("No hits found.")
        # Query ES using requests and RESTful API
        else:
            logging.info(f"→→→ Query {query_string} on ES using requests")
            # Perform the search request
            url = f"{url}/{index_name}/_search"
            response = requests.get(url, json=query_body)

            # Process the search results
            if response.status_code == 200:
                results = response.json()
                hits = results["hits"]["hits"]
                for hit in hits:
                    source = hit["_source"]
                    highlight = hit.get("highlight", {})
                    highlighted_field = highlight.get(field_name, [])
                    logging.info("→→→ Highlights:")
                    logging.info(highlighted_field)

            else:
                logging.error("Search request failed.")
        end_time = time.time()
        self.log_time_records("highlight_elasticsearch", start_time, end_time)

    # Run a query on OpenSearch with highlights
    # Client: TEST | Requests: TODO
    @log_execution_time
    def highlight_opensearch(self, url: str, index_name:str, query_str: str, field_name: str, proximity_distance=None):
        start_time = time.time()
        os_client = self.client
        type = "plain"
         # Build the query body based on the query type
        query_body = {
            "query": {},
            "highlight": {
                "fields": {
                    field_name: {}
                },
                "type": type,
                "pre_tags": "→→→",
                "post_tags": "←←←",
                "number_of_fragments": 10,
                "fragment_size": 100,
            }
        }

        if proximity_distance is None:
            print("Proximity distance not provided. Treat as match_phrase")
            # Perform match_phrase query
            query_body["query"] = {
                "match_phrase": {
                    field_name: query_str
                }
            }
        else:
            query_body["query"] = {
                "match_phrase": {
                    field_name: {
                        "query": f'"{query_str}"',
                        "slop": proximity_distance
                    }
                }
            }
        # Python client implementation
        if os_client:
            try:
                response = os_client.search(index=index_name, body=query_body)
                if response:
                    # Process the search results
                    if "hits" in response:
                        hits = response["hits"]["hits"]
                        logging.info("→→→→→→→ Highlights:")
                        for hit in hits:
                            text_content = hit["_source"][field_name]
                            if text_content:
                                text_content = text_content[0:50]
                                logging.info(text_content)
                            # Access the highlighted content
                            highlight = hit.get("highlight", {})
                            highlighted_field = highlight.get(field_name, [])
                            # Print the highlighted content
                            logging.info("→→→ Highlights:")
                            for i, highlight in enumerate(highlighted_field[:10]):
                                logging.info(f"\n →→→ Highlight {i+1}: \n{highlight}\n")
                    else:
                        logging.info("No hits found.")
                else:
                    logging.info("*** Bad response.")
            except Exception as e:
                logging.error("* Highlight error")
                logging.error(e)
        # Requests implementation
        else:
            logging.info("Running search query using Requests")

        end_time = time.time()
        self.log_time_records("highlight_opensearch", start_time, end_time)


    # Method to make a complex query with highlighting in SOLR
    @log_execution_time
    def complex_query_highlight_solr(self, full_string, additional_terms):
        start_time = time.time()
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
        end_time = time.time()
        self.log_time_records("complex_query_highlight_solr", start_time, end_time)

    # Delete all documents from the SOLR collection
    # Client: Ok | Requests: Ok
    @log_execution_time
    def delete_solr(self, url):
        # Fetch client
        solr_client = self.client
        # Client deletion:
        if solr_client:
            query = "*:*"  # Match documents with id iqual to anything, so all documents
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
            delete_url = url + "/update/"

            delete_query = {"delete": {"query": "*:*"}}
            # Commit True is passed to ensure the operation is commited immediately
            query_params = {"commit": "true"}

            try:

                logging.info(f"url: {delete_url}")
                logging.info(f"query: {delete_query}")
                response = requests.post(
                    delete_url, json=delete_query, params=query_params
                )
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

    # Delete all documents from the ElasticSearch collection
    # Client: Ok | Requests: Ok
    @log_execution_time
    def delete_elasticsearch(self, index_name):
        url = f"http://localhost:9200/{index_name}/_delete_by_query?conflicts=proceed"

        # Set request headers
        headers = {"Content-Type": "application/json"}

        # Set the query payload
        query = {"query": {"match_all": {}}}

        es = self.client
        # Client deletion:
        if es:
            logging.info("* Deleting using ES client.")
            response = es.delete_by_query(index=index_name, body=query)
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
                logging.info(
                    f"Deleted {deleted_count} documents from index '{index_name}'"
                )
                return deleted_count
            else:
                logging.error(f"Error: {response.content}")
                return 0

    # Delete all documents from the OpenSearch index
    # Client: TEST | Requests: TEST
    @log_execution_time
    def delete_opensearch(self, url,  index_name):
        url = f"{url}/{index_name}/_delete_by_query"

        # Set request headers
        headers = {"Content-Type": "application/json"}

        # Set the query payload
        query = {"query": {"match_all": {}}}  # Match all documents

        os_client = self.client
        # Client deletion:
        if os_client:
            logging.info("* Deleting using OS client.")
            response = os_client.delete_by_query(index=index_name, body=query)
            # Check if the delete operation was successful
            n_deleted = response["deleted"]
            if n_deleted:
                logging.info(f"→→→ {n_deleted} documents deleted successfully.")
            else:
                logging.error("No documents deleted.")
        # Requests alternative
        else:
            logging.info("* Deleting using requests - Is not implemented")

    # Method for retrieving collection and field information in SOLR
    # Client: TODO | Requests: TEST
    @log_execution_time
    def get_field_information_solr(self, solr_url, field_name):
        # Prepare the URL for the field information request
        url = f"{solr_url}/schema/fields/{field_name}"

        try:
            # Send the GET request to fetch the field information
            response = requests.get(url)
            # Check the response status
            if response.status_code == 200:
                field_info = json.dumps(response.json(), indent=3)
                # logging.info(f"Field analyzer: {field_info['analyzer']}")
                logging.info(f"→→→ Field: {field_info}")
                return response.status_code
            else:
                logging.error(
                    f"Error fetching field information: {response.status_code} - {response.text}"
                )
                return None
        except Exception as e:
            logging.error("Get error field information error.")
            logging.error(e)

    @log_execution_time
    def get_server_status_elasticsearch(self):
        health = self.client.cluster.health()
        health_dump = json.dumps(health, indent=3)
        logging.info("→→→ ES Client Health Status:")
        logging.info(health_dump)
        return health_dump

    @log_execution_time
    def get_settings_from_elasticsearch(self, index_name):
        es_client = self.client
        logging.info("**************************")
        # Extract the analyzer information from the index settings
        index_info = es_client.indices.get(index=index_name)
        settings_info = index_info[index_name]["settings"]
        if settings_info:
            settings_info = json.dumps(settings_info, indent=3)
            logging.info(f"→→→ Index [{index_name}] Settings:")
            logging.info(settings_info)
        else:
            logging.error("→→→ No settings available!")

    # Method for retrieving collection and field information in SOLR
    # Client: Ok | Requests: TODO
    @log_execution_time
    def get_field_information_elasticsearch(self, url, index_name, field_name):
        # Retrieve client from object
        es_client = self.client
        # Get it using client
        if es_client:
            logging.info(f"→→→ Get information using ES Client")
            # Get the mapping information for the specified index
            mapping = es_client.indices.get_mapping(index=index_name)
            mapping_dump = json.dumps(mapping, indent=3)
            # logging.info(mapping_dump)
            # Extract the analyzer information from the mapping
            mapping_info = mapping[index_name]["mappings"]["properties"]
            if mapping_info:
                mapping_info = json.dumps(mapping_info, indent=3)
                logging.info(f"→→→ Field mapping info for index [{index_name}]")
                # logging.info(mapping_info)
                try:
                    # get keys from dict and check if field_name is in it:
                    dict_keys = json.loads(mapping_info).keys()
                    if dict_keys:
                        mapping_info = json.loads(mapping_info)
                        logging.info(
                            f"→→→ Mapping - Fields available info for [{index_name}]:"
                        )
                        logging.info(dict_keys)
                        field_present = False
                        for item in dict_keys:
                            if field_name in item:
                                logging.info(
                                    f"→→→ Mapping - Fields information available info for [{field_name}]:"
                                )
                                field_info = json.dumps(
                                    mapping_info[field_name], indent=3
                                )
                                logging.info(field_info)
                                field_present = True
                        if not field_present:
                            logging.info(
                                f"→→→ Mapping - No information available info for [{field_name}]:"
                            )
                    else:
                        logging.error(f"*** No fields for [{index_name}]")
                except Exception as e:
                    logging.error(e)
            else:
                logging.info("→→→ No mappings available!")
        # Get information using Requests
        else:
            logging.info(f"→→→ Get information using Requests")

    # Sets the ElasticSearch Analyzer to Portuguese (pt-br)
    # Client: Ok | Requests: TEST
    @log_execution_time
    def set_pt_br_analyzer_elasticsearch(self, hosts: List, index_name: str):
        # Configurações trazidas do método  create_index(index: IndexInterface) -> None: do querido-diario-data-processing/tasks/gazette_text_extraction.py
        mappings = {
            "properties": {
                "id": {"type": "keyword"},
                "content_br": {
                    "type": "text",
                    "analyzer": "brazilian_with_stopwords",
                    "index_options": "offsets",
                    "term_vector": "with_positions_offsets",
                },
                "content_en":  {
                    "type": "text",
                    "analyzer": "exact",
                    "index_options": "offsets",
                    "term_vector": "with_positions_offsets",
                },
            }
        }
        settings = {
            "analysis": {
                "filter": {
                    "brazilian_stemmer": {
                        "type": "stemmer",
                        "language": "brazilian",
                    }
                },
                "analyzer": {
                    "brazilian_with_stopwords": {
                        "tokenizer": "standard",
                        "filter": ["lowercase", "brazilian_stemmer"],
                    },
                    "exact": {
                        "tokenizer": "standard",
                        "filter": ["lowercase"],
                    },
                },
            }
        }

        body = {"mappings": mappings, "settings": settings}

        es_client = self.client
        if es_client:
            try:
                logging.info("*** Updating index settings with ES client")
                # delete index if exists
                if es_client.indices.exists(index_name):
                    es_client.indices.delete(index=index_name)
                # Create index with fields
                response = es_client.indices.create(index=index_name, body=body)
                logging.info("→→→ Response for index settings:")
                logging.info(response)

            except Exception as e:
                logging.error("Index settings error.")
                logging.error(e)
        # Run with requests:
        else:
            url = f"{hosts[0]}/{index_name}/"
            logging.info(f"*** Running index settings with requests. Base URL: [{url}]")
            # Create the index with the Brazilian analyzer configuration
            response = requests.put(f"{url}", json=json.dumps(body))
            if response.status_code == 200:
                logging.info(
                    f"Successfully created the index '{index_name}' with the Brazilian analyzer for field content_br."
                )
            else:
                logging.error(f"Failed to update the index '{index_name}'.")
        return response

    # Sets the SOLR Analyzer to Portuguese (PT-br)
    # Client: NO | Requests: Ok
    # This was done via Web Interface, not code
    @log_execution_time
    def set_analyzers_solr(self, solr_url):
        solr_client = self.client

        # Prepare the URL for the fieldType update request
        endpoint = "http://localhost:8983/api/collections/solr_index/schema/"
        # Prepare headers
        headers = {"Content-Type": "application/json"}
        payload_br = {
            # Here, there could be other operations, such as replace(update) or remove (delete)
            "add-field": {
                "name": "content_br",
                "type": "text_pt", #text_general
                "default": "br",
                "stored": True,
                "indexed": True,
                "multiValued": False
            }
        }

        payload_en = {
            # Here, there could be other operations, such as replace(update) or remove (delete)
            "add-field": {
                "name": "content_en",
                "type": "text_general", #text_general
                "stored": True,
                "indexed": True,
                "multiValued": False
            }
        }

        # Run using pysolr Client
        if solr_client:
            try:
                logging.info(f"*** No request available for schemas in pysolr at the moment.")
            except Exception as e:
                logging.error("Error changing field type:")
                logging.error(e)
        # Running on Requests
        else:
            logging.info(f"*** Setting SOLR with Portugese BR Analyzer for field {field_name} using requests")
            try:
                # Add content_br field to collection schema:
                response = requests.post(endpoint, data=json.dumps(payload_br), headers=headers)
                # response.raise_for_status()
                response_json = json.dumps(response.json(), indent=3)
                if response_json['response_header']['status'] == 0:
                    logging.info(f"************ Anayzer added field content_br correctly. Response from index collection update:")
                    logging.info(response_json)
                else:
                    logging.error(f"************ There was an error adding content_br.")

                # Add content_br field to collection schema:
                response = requests.post(endpoint, data=json.dumps(payload_en), headers=headers)
                # response.raise_for_status()
                response_json = json.dumps(response.json(), indent=3)
                if response_json['response_header']['status'] == 0:
                    logging.info(f"************ Anayzer added field content_en correctly. Response from index collection update:")
                    logging.info(response_json)
                else:
                    logging.error(f"************ There was an error adding content_en.")

            except requests.exceptions.HTTPError as e:
                print(f"HTTP error occurred: {e}")
            except requests.exceptions.RequestException as e:
                print(f"An error occurred: {e}")


    # Sets the OpenSearch Analyzer to Portuguese (pt-br)
    # Client: TODO | Requests: TODO
    @log_execution_time
    def set_analyzers_opensearch(self, hosts: List, index_name: str):
        os_client = self.client
        if os_client:
            try:
                logging.info("*** Updating index settings with OS client")
                # Specify the mapping properties for the index
                # Specify the mapping properties for the index
                mapping_properties = {
                    "mappings": {
                        "properties": {
                            "content_br": {
                                "type": "text",
                                "analyzer": "portuguese",
                                "term_vector": "with_positions_offsets"
                            },
                            "content_en": {
                                "type": "text",
                                "analyzer": "standard",
                                "term_vector": "with_positions_offsets"
                            }
                        }
                    }
                }

                # Delete if exists:
                if  os_client.indices.exists(index=index_name):
                    response = os_client.indices.delete(index=index_name)
                # Create the index with the specified mappings
                response = os_client.indices.create(index=index_name, body=mapping_properties)

                if response:
                    logging.info("→→→ Response for index settings:")
                    logging.info(response)
                    print(f"Mappings set successfully for index '{index_name}'.")
                else:
                    print(f"Failed to set mappings for index '{index_name}'.")

            except Exception as e:
                logging.error("Index settings error.")
                logging.error(e)
        # Run with requests:
        else:
            url = f"{hosts[0]}/{index_name}/"
            logging.info(f"*** Running index settings with requests. Base URL: [{url}]")
            # Create the index with the Brazilian analyzer configuration
            response = requests.put(f"{url}", json=json.dumps(body))
            if response.status_code == 200:
                logging.info(
                    f"Successfully updated the index '{index_name}' with the Brazilian analyzer."
                )
            else:
                logging.error(f"Failed to update the index '{index_name}'.")
        return response

    # Commit solr operations
    def commit_solr(self):
        url = "http://localhost:8983/solr/solr_index/update?commit=true"
        headers = {"Content-Type": "application/json"}
        response = requests.post(url, headers=headers)
        if response.status_code == 200:
            print("Commit successful")
        else:
            print("Error committing to Solr")

    def close_connections(self):
        exit_client = self.client
        if exit_client:
            exit_client.close()

    # This utilitary method receives a content string, a phrase string to be searched and the expected return is a string of size chars, where the phrase appears in the center of the returned string.
    def get_centered_fragment(self, content: str, size: int, phrase: str):
        try:
            content_length = len(content)
            phrase_length = len(phrase)

            if size >= content_length:
                return content

            if size <= phrase_length:
                return phrase

            half_size = (size - phrase_length) // 2

            phrase_index = content.find(phrase)
            # Alguns casos com caracteres como \n a busca não está funcionando corretamente.
            if phrase_index == -1:
                return ""

            start_index = max(0, phrase_index - half_size)
            end_index = min(content_length, start_index + size)

            return content[start_index:end_index]
        except Exception as e:
            logging.error(
                f"Error in get_centered_fragment() size: {size} | phrase: {phrase}"
            )
            logging.error(e)

    # This method is supposed to receive a query,
    # run it on the Querido Diário API
    # Download all the results from text_url parameter in the response results
    @log_execution_time
    def download_txt_from_qd(self, query_string: str, city: str, data_directory: str):
        # TODO: This is gonna get only the first 1000 results. To continue, implement a loop that adds the field offset with the value 1000 each round, until the response is no longer valid
        api_url = f"https://queridodiario.ok.org.br/api/gazettes?querystring={query_string}&excerpt_size=100&number_of_excerpts=10&pre_tags=%E2%86%92%E2%86%92%E2%86%92&post_tags=%E2%86%90%E2%86%90%E2%86%90&size=1000&sort_by=relevance"
        logging.info(
            f"*** Download text files from QD API for the search string: [{query_string}] for the city {city}"
        )
        try:
            # Prepare headers
            headers = {"Content-Type": "application/json"}

            # Get current status:
            response = requests.get(api_url, headers=headers)
            if response:
                response_json = response.json()
                if response_json:
                    total = response_json['total_gazettes']
                    logging.info(f"*** {total} diários.")
                    for gazette in response_json['gazettes']:
                        if gazette:
                            # logging.info(json.dumps(gazette, indent=3))
                            text_url = gazette['txt_url']
                            logging.info(text_url)
                            # TODO: Implement downloading txt files to disk
                            self.download_txt_gazette(text_url, data_directory)
                            excerpts = gazette['excerpts']
                            if excerpts is not None:
                                for e in excerpts:
                                    logging.info(e)
                    logging.info("Current response for download_txt_from_qd():")
                    logging.info()
        except Exception as e:
            logging.error(e)

    # Auxiliary method simply receives a url and makes the request to download to the local destination provided
    def download_txt_gazette(self, text_url: str, local_path: str):
        try:
            # Prepare headers
            headers = {"Content-Type": "application/json"}
            response = requests.get(text_url, headers=headers)
            # Raise an exception if the request was unsuccessful
            response.raise_for_status()
            if response:
                # Extract file name from url and add it t the destination_path:
                file_name = text_url.split("/")[-1]
                destination_path = local_path + file_name
                try:
                    with open(destination_path, 'wb') as file:
                        # Write down bytes to the local .txt file
                        file.write(response.content)
                        logging.info(f"*** Arquivo {file_name} salvo com sucesso em disco.")
                except requests.exceptions.HTTPError as e:
                    print(f"HTTP error occurred: {e}")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
