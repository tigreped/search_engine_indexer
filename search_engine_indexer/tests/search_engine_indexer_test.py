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
opensearch_hosts = ["http://localhost:9201"]
opensearch_index = "opensearch_index"

# SOLR credentials
solr_host_index = ["http://localhost:8983/solr/solr_index"]

# Directory with txt files to be indexed
data_directory = "../../extraction/docs/txt/"

# Test search functionality for a simple phrase query
# OBS.: Os testes na API do QD contra o ElasticSearch indicam que dentro do python, o correto para passar query phrases é utilizar aspas simples mais externas e aspas cuplas internas com a frase a buscar. Não é necessário utilizar contrabarra
query = "violência contra mulher"
# query = "contribuições apontadas pela sociedade"
# query = "projeto combate violência mulher"
# query = '"licenciamento ambiental" processo dispensa'
# query = '"aprovação do projeto" + "alvará de licença" + "Relação de Bens Patrimoniais"'
# Cosnsulta com frases distantes entre si e termos distantes entre si, utilizando o multi_phrase para a query do elasticsearch
# query = '(\"proprietário obs notificado\") + (\"através \nda NOTIFICAÇÃO\") + (\"infringindo o disposto\")'


def solr_test():
    ### SOLR Indexing and querying ###
    # Create a SOLR instance of SearchEngineIndexer
    try:
        solr_indexer = SearchEngineIndexer("SOLR", solr_host_index)
        #solr_indexer.download_txt_from_qd(query, None, data_directory)
        # Deleting index:
        # solr_indexer.delete_solr(solr_host_index[0])
        #
        # # Process and index the files
        # logging.info("Starting to process files to SOLR index")
        # solr_indexer.process_and_index_files(data_directory)
        # logging.info("Done processing files to SOLR index")
        # Search the SOLR server
        if solr_indexer:
            logging.info("→→→ Testing SOLR.")

            # Set analyzer for Portuguese.
            # solr_indexer.get_field_information_solr(solr_host_index[0], "content")
            # solr_indexer.get_field_information_solr(solr_host_index[0], "content_en")
            # solr_indexer.get_field_information_solr(solr_host_index[0], "content_br")
            # solr_indexer.set_pt_br_analyzer_solr(solr_host_index[0], "content_en")
            # solr_indexer.get_field_information_solr(solr_host_index[0], "content")

            # solr_indexer.query_with_solr("violência doméstica")
            # solr_indexer.query_with_solr('"(violência) (contra) (mulher)" ~5')
            solr_indexer.highlight_solr(query, "content_br")
            # solr_indexer.complex_query_highlight_solr("violência contra mulheres", ["pretas", "pardas", "de cor", "indígenas", "estrangeiras", "imigrantes"])
            # solr_indexer.complex_query_highlight_solr("violência contra", ["mulheres", "combate"]) # "ambiente familiar"
            # solr_indexer.add_document_solr(payload)
            formatted_json = json.dumps(solr_indexer.time_records, indent=3)
            logging.info(f"→→→ Tempos: {formatted_json}")
    except Exception as e:
        logging.error(e)


def elasticsearch_test():
    ### Elastic Search Indexing and querying ###

    # Create an instance of SearchEngineIndexer
    try:
        es_indexer = SearchEngineIndexer("ES", elasticsearch_hosts, elasticsearch_index)

        # Process and index the files
        try:
            # Set pt_br analyzer
            # es_indexer.set_pt_br_analyzer_elasticsearch(elasticsearch_hosts[0], elasticsearch_index)
            # es_indexer.set_pt_br_analyzer_elasticsearch(elasticsearch_hosts[0], elasticsearch_index)

            # Delete index documents:
            # deleted = es_indexer.delete_elasticsearch(elasticsearch_index)
            # logging.info(f'* Deleted documents: {deleted}')

            # Index files from directory:
            # logging.info("→→→ Starting to process files to ES index")
            # es_indexer.process_and_index_files(data_directory)
            # logging.info("←←← Done processing files to ES index")

            # Test Query with phrases
            # logging.info("→→→ Query ES index")
            # es_indexer.query_with_elasticsearch(query)

            # Highlight query
            # Com slop funciona para os match_phrase, mas sem o slop deixa de retornar highlighst para o fvh
            es_indexer.highlight_elasticsearch(elasticsearch_hosts[0], elasticsearch_index, query, "content_br", 10)
            # logging.info("\n\n**********************\n\n")
            # es_indexer.highlight_elasticsearch(elasticsearch_index, query, "content_br")
            # Get info from the index
            # es_indexer.get_field_information_elasticsearch(elasticsearch_hosts, elasticsearch_index, "content")
            # es_indexer.get_field_information_elasticsearch(elasticsearch_hosts, elasticsearch_index, "content")

            # es_indexer.get_field_information_elasticsearch(elasticsearch_hosts, elasticsearch_index, "content_br")
            # es_indexer.get_field_information_elasticsearch(elasticsearch_hosts, elasticsearch_index, "content")
            formatted_json = json.dumps(es_indexer.time_records, indent=3)
            logging.info(f"→→→ Tempos: {formatted_json}")
        except Exception as e:
            logging.error(e)
    except Exception as e:
        logging.error(e)

    finally:
        if es_indexer:
            # Close connections
            es_indexer.close_connections()


### Open Search Indexing and querying ###
def opensearch_test():
    # Create an instance of SearchEngineIndexer
    os_indexer = SearchEngineIndexer("OS", opensearch_hosts, opensearch_index)

    # OS client connections
    opensearch = os_indexer.client
    if opensearch is not None:
        # Search using OpenSearch
        logging.info("→ OpenSearch client successful. Testing OS query:")
        try:
            # logging.info("→→→ Starting to process files to OS index")
            # os_indexer.process_and_index_files(data_directory)
            # logging.info("←←← Done processing files to OS index")
            # os_indexer.query_with_opensearch(query, opensearch_index)

            # Set languages:
            # os_indexer.set_analyzers_opensearch(opensearch_hosts, opensearch_index)

            # Process and index the files
            # logging.info("→→→ Starting to process files to OS index")
            # os_indexer.process_and_index_files(data_directory)
            # logging.info("←←← Done processing files to OS index")

            # Test query with highlights:
            # os_indexer.highlight_opensearch(opensearch_hosts[0], opensearch_index, query, "content_br")
            os_indexer.highlight_opensearch(opensearch_hosts[0], opensearch_index, query, "content_br", 10)

            formatted_json = json.dumps(os_indexer.time_records, indent=3)
            logging.info(f"→→→ Tempos: {formatted_json}")
        except Exception as e:
            logging.error(e)
        finally:
            # Close the connections
            os_indexer.close_connections()

## Enable the desired tests to r by uncommenting the line: ##

# solr_test()
# elasticsearch_test()
opensearch_test()
