from search_engine_indexer import SearchEngineIndexer
from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch
import logging
import requests
import json

# Run tests with: python -m unittest discover -s tests -p "*_test.py"

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ES credentials
elasticsearch_hosts = ["http://localhost:9200"]
elasticsearch_index = "elasticsearch_index"

# OS credentials
opensearch_hosts = ["http://localhost:9202"]
opensearch_index = "opensearch_index"

# SOLR credentials
solr_host_index = ["http://localhost:8983/solr/solr_index"]

# Directory with txt files to be indexed
data_directory = "../../extraction/docs/txt/"

# Test search functionality for a simple phrase query
#query = '"violência contra mulher mulheres"'
query = '"licenciamento ambiental" processo dispensa'

# Search query body
query_body = {"query": {"match": {"content": query}}}

def solr_test():
    ### SOLR Indexing and querying ###
    # Create a SOLR instance of SearchEngineIndexer
    try:
        solr_indexer = SearchEngineIndexer(None, 'SOLR', solr_host_index)
        # Process and index the files

        #logging.info("Starting to process files to SOLR index")
        #solr_indexer.process_and_index_files(data_directory)
        #logging.info("Done processing files to SOLR index")
        # Search the SOLR server
        logging.info("→ Testing SOLR query")
        #solr_indexer.query_with_solr("violência contra mulheres")
        #solr_indexer.highlight_solr("violência contra")
        #solr_indexer.complex_query_highlight_solr("violência contra mulheres", ["pretas", "pardas", "de cor", "indígenas", "estrangeiras", "imigrantes"])
        solr_indexer.complex_query_highlight_solr("violência contra", ["mulheres", "combate"]) # "ambiente familiar"
    except Exception as e:
        logging.error(e)

def elasticsearch_test():
    ### Elastic Search Indexing and querying ###

    # Create an instance of SearchEngineIndexer
    try:
        es_indexer = SearchEngineIndexer(
            None, "ES", elasticsearch_hosts, elasticsearch_index
        )

        # Process and index the files
        try:
            # Set pt_br analyzer
            #es_indexer.set_pt_br_analyzer_elasticsearch(elasticsearch_hosts[0], elasticsearch_index)

            # Index files from directory:
            #logging.info("→→→ Starting to process files to ES index")
            #es_indexer.process_and_index_files(data_directory)
            #logging.info("←←← Done processing files to ES index")

            # Delete index documents:
            #deleted = es_indexer.delete_elasticsearch(elasticsearch_index)
            logging.info(f'* Deleted documents: {deleted}')

            # Test Query with phrases
            #logging.info("→→→ Query ES index")
            #es_indexer.query_with_elasticsearch(query)

            # Highlight query
            es_indexer.highlight_elasticsearch(elasticsearch_index, query)
            logging.info(f'→→→ Tempos: {es_indexer.time_records}')

        except Exception as e:
            logging.error(e)


    finally:
        if es_indexer:
            # Close connections
            es_indexer.close_connections()


### Open Search Indexing and querying ###
def opensearch_test():
    # Create an instance of SearchEngineIndexer
    os_indexer = SearchEngineIndexer(None, 'OS', opensearch_hosts, opensearch_index)
    # Process and index the files
    #logging.info("→→→ Starting to process files to OS index")
    #os_indexer.process_and_index_files(data_directory)
    #logging.info("←←← Done processing files to OS index")
    # OS client connections
    opensearch = os_indexer.client
    if opensearch is not None:
        # Search using OpenSearch
        logging.info("→ OpenSearch client successful. Testing OS query:")
        try:
            # TODO: move to method
            os_indexer.query_with_opensearch(query, opensearch_index)
            logging.info(f'→→→ Tempos: {os_indexer.time_records}')
        except Exception as e:
            logging.error(e)
        finally:
            # Close the connections
            os_indexer.close_connections()

# Running the following tests:
elasticsearch_test()
