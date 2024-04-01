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

    def __init__(self, es_url, es_api_key, index_name):
        self.client = Elasticsearch([es_url], api_key=es_api_key)
        self.index_name = index_name


    def fetch_aggregation_results(self, query):
        response = self.client.search(index=self.index_name, body=query)
        buckets = response["aggregations"]["results"]["buckets"]
        return pd.DataFrame(buckets).sort_values(by='doc_count')

    def calculate_progress(self, selected_script):
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
        search = Search(using=self.client, index=self.index_name).query("bool", must=[{"match": {"status": "new"}}, {"match": {"meta.script.keyword": selected_script}}]).sort("_doc")[:1]
        response = search.execute()
        if response.hits.total.value > 0:
            document = response[0]
            self.update_document_status(document.meta.id, "in progress", nickname)
            return document
        return None

    def update_document_status(self, doc_id, status, nickname):
        update_body = {"doc": {"status": status, "last_modified": datetime.now().isoformat()}}
        if nickname:
            update_body["doc"]["updated_by"] = nickname
        self.client.update(index=self.index_name, id=doc_id, body=update_body)

    def update_document(self, doc_id, update_body):
        self.client.update(index=self.index_name, id=doc_id, body=update_body)

    def get_scripts(self):
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
    
