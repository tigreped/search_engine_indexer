from search_engine_indexer import SearchEngineIndexer
from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch
import logging
import requests
import json

# Run tests with: python -m unittest discover -s tests -p "*_test.py"

# Initialize logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# ES credentials
elasticsearch_hosts = ['https://localhost:9200']
elasticsearch_index = 'elastic_index'
# OS credentials
opensearch_hosts = ['localhost:9222']
opensearch_index = 'opensearch_index'
# SOLR credentials
solr_host_index = ['http://localhost:8983/solr/solr_index'] # TODO with or without '/#/'

# Directory with txt files to be indexed
data_directory = '../../extraction/docs/txt/'

# Test search functionality for a simple query
query = 'violência contra mulheres'

# Search query body
query_body =  {'query': {'match': {'content': query}}}

### SOLR Indexing and querying ###
# Create a SOLR instance of SearchEngineIndexer
try:
    solr_indexer = SearchEngineIndexer(None, 'SOLR', solr_host_index)
    # Process and index the files
    '''
    logging.info("Starting to process files to SOLR index")
    solr_indexer.process_and_index_files(data_directory)
    logging.info("Done processing files to SOLR index")
    '''
    # Search the SOLR server
    logging.info("→ Testing SOLR query")
    #solr_indexer.query_with_solr("violência contra mulheres")
    solr_indexer.highlight_solr("violência contra mulheres")
except Exception as e:
    logging.error(e)

### Elastic Search Indexing and querying ###
'''
# Create an instance of SearchEngineIndexer
try:
    es_indexer = SearchEngineIndexer(None, 'ES', elasticsearch_hosts, elasticsearch_index)
    # Process and index the files
    logging.info("Starting to process files to ES index")

    es_indexer.process_and_index_files(data_directory)
    logging.info("Done processing files to ES index")
    # ES client connection
    es = es_index.client
    if es is not None:
        # Search the ES server
        logging.info("→ Testing ES query")
        es_results = es.search(index=elastic_search_index, body=query_body)
        print("Elasticsearch Results:")
        for hit in es_results['hits']['hits']:
            print(hit['_score'], hit['_source'])
    # Close connection:
    es_indexer.close_connections()
except Exception as e:
    logging.error(e)
'''

### Open Search Indexing and querying ###
'''
# Create an instance of SearchEngineIndexer
os_indexer = SearchEngineIndexer(None, 'OS', opensearch_hosts, opensearch_index)
# Process and index the files
logging.info("Starting to process files to OS index")
os_indexer.process_and_index_files(data_directory)
logging.info("Done processing files to OS index")
# OS client connections
opensearch = os_indexer.client
if opensearch is not None:
    # Search using OpenSearch
    logging.info("→ Testing OS query")
    opensearch_results = opensearch.search(index=opensearch_index, body=query_body)
    print("OpenSearch Results:")
    for hit in opensearch_results['hits']['hits']:
        print(hit['_score'], hit['_source'])
# Close the connections
os_indexer.close_connections()
'''
