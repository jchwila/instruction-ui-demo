import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

def anonymize_email(email):

    local, domain = email.split('@')
    domain_name, extension = domain.split('.')
    anonymized_local = local[0] + "*" * (len(local) - 1)
    anonymized_domain_name = domain_name[0] + "*" * (len(domain_name) - 1)
    anonymized_email = f"{anonymized_local}@{anonymized_domain_name}.{extension}"
    return anonymized_email

class ElasticsearchDataHandler:

    def __init__(self, es_url, es_api_key, index_name='instructions-demo'):
        self.client = Elasticsearch(es_url, api_key=es_api_key)
        self.index_name = index_name


    def fetch_aggregation_results(self, query):

        """
        Executes a search query that includes aggregations and returns the results as a pandas DataFrame.
        
        Parameters:
        - query (string): KQL query to execute.
        """        
        response = self.client.search(index=self.index_name, body=query)
        buckets = response["aggregations"]["results"]["buckets"]
        return pd.DataFrame(buckets).sort_values(by='doc_count')
    


    def calculate_progress(self, selected_script):
        
        """
        Calculates the progress of selected instruction subset (selected_script).
        It uses a predefined KQL query to aggregate document statuses and returns calculated progress as a fraction.
               
        Parameters:
        - selected_script (string): value for 'script' field in Elsticsearch instruction metadata defining instructions subset.
        """ 

        progress_query = {
        "size": 0,  
        "query": {
            "bool": {
            "filter": [
                {
                "term": {
                    "meta.script.keyword": selected_script
                }
                }
            ]
            }
        },
        "aggs": {
            "results": {
            "terms": {
                "field": "status.keyword" }
            }
        }
        }
        df = self.fetch_aggregation_results(progress_query)
        total_docs = df['doc_count'].sum()
        total_except_new = df[df['key'] != 'new']['doc_count'].sum()
        return total_except_new / total_docs if total_docs else 0


    def leaderboard_df(self, selected_script):

        """
        Creates a leaderboard DataFrame from Elasticsearch data based on the updated_by field. 
        This function anonymizes email addresses in the 'key' column, making it suitable for public display of user contributions.
               
        Parameters:
        - selected_script (string): value for 'script' field in Elsticsearch instruction metadata defining instructions subset.
        """ 


        leaders_query = {
                        "size": 0,  
                        "query": {
                            "bool": {
                            "filter": [
                                {
                                "term": {
                                    "meta.script.keyword": selected_script
                                }
                                }
                            ]
                            }
                        },
                            "aggs": {
                                "results": {
                                    "terms": {
                                        "field": "updated_by.keyword"
                                    }
                                }
                            }
                        }
        df = self.fetch_aggregation_results(leaders_query)
        df['key'] = df['key'].apply(lambda x: anonymize_email(x) if '@' in x and '.' in x else x)
        return df.sort_values(by='doc_count', ascending=True)


    def get_next_document(self, selected_script, nickname):

        """
        Fetches the next instruction document marked as 'new' for a selected instruction subset and updates its status to 'in progress'. Retruns json.
               
        Parameters:
        - selected_script (string): value for 'script' field in Elsticsearch instruction metadata defining instructions subset.
        - nickname (string): username value to save in updated_by field of instruction document.
        """ 

        search = Search(using=self.client, index=self.index_name).query("bool", must=[{"match": {"status": "new"}}, {"match": {"meta.script.keyword": selected_script}}]).sort("_doc")[:1]
        response = search.execute()
        if response.hits.total.value > 0:
            document = response[0]
            self.update_document_status(document.meta.id, "in progress", nickname)
            return document
        return None

    def update_document_status(self, doc_id, status, nickname):

        """
        Updates the status and last_modified value of a selected instruction document and optionally the nickname of the user who updated it. 
               
        Parameters:
        - doc_id (string): unique Elasticsearch document identifier
        - status (string): status value to set. 'ok', 'not ok', 'in progress'
        - nickname (string): username value to save in updated_by field of instruction document.
        """ 

        update_body = {"doc": {"status": status, "last_modified": datetime.now().isoformat()}}
        if nickname:
            update_body["doc"]["updated_by"] = nickname
        self.client.update(index=self.index_name, id=doc_id, body=update_body)

    def update_document(self, doc_id, update_body):
        
        """
        Allows for arbitrary updates to a instruction document using a specified update body json. 
               
        Parameters:
        - doc_id (string): unique Elasticsearch document identifier
        - update_body: json with new values for instruction fields.
       """   
        
        self.client.update(index=self.index_name, id=doc_id, body=update_body)

    def get_scripts(self):

        """
        Fetches and returns a list of unique script field values from all documents in index. 
               
        Parameters:
        - doc_id (string): unique Elasticsearch document identifier
        - update_body: json with new values for instrunction fields.
       """  


        scripts_query = {
                            "size": 0,
                            "aggs": {
                                "unique_scripts": {
                                    "terms": {
                                        "field": "meta.script.keyword",  
                                        "size": 1000  
                                    }
                                }
                            }
                        }
        response = self.client.search(index=self.index_name, body=scripts_query)
        unique_scripts = [bucket['key'] for bucket in response['aggregations']['unique_scripts']['buckets']]
        return unique_scripts
    
    def get_instructions(self):

        """
        Fetches all instructions from an Elasticsearch index where status field value is 'ok',
        returning only the 'instruction' part of each document.

        """

        page = self.client.search(
        index=self.index_name,
        scroll='5m',
        size=10000,
        query={
            "bool": {
                "must": [
                    {
                        "term": {
                            "status.keyword": "ok"
                                }
                    }
                        ]       
                    }
                })



        scroll_id = page['_scroll_id']
        all_instructions = [hit['_source']['instruction'] for hit in page['hits']['hits']]

        while len(page['hits']['hits']):
            page = self.client.scroll(scroll_id=scroll_id, scroll='2m')
            scroll_id = page['_scroll_id']
            all_instructions.extend(hit['_source']['instruction'] for hit in page['hits']['hits'])

        self.client.clear_scroll(scroll_id=scroll_id)
        
        return all_instructions
        
