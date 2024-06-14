import os
import json
from typing import List, Optional, Dict, Any, Union
from requests_toolbelt import MultipartEncoder
import requests
import random

class ZillizConfig:
    def __init__(self,
        project_id: str = None,
        cluster_id: str = None,
        api_key: str = None,
        cloud_region: str = "ali-cn-hangzhou"):
        self.project_id = os.getenv("ZILLIZ_PROJECT_ID") or project_id 
        self.cluster_id = os.getenv("ZILLIZ_CLUSTER_ID") or cluster_id
        self.api_key = os.getenv("ZILLIZ_TOKEN") or api_key
        self.cloud_region = cloud_region

def list_pipelines(cfg, collection_name: str):
    url = f"https://controller.api.{cfg.cloud_region}.cloud.zilliz.com.cn/v1/pipelines?projectId={cfg.project_id}"
    headers = {
        "Authorization": f"Bearer {cfg.api_key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    collection_name = collection_name
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise RuntimeError(response.text)
    response_dict = response.json()
    if response_dict["code"] != 200:
        raise RuntimeError(response_dict)
    pipeline_ids = {}
    for pipeline in response_dict['data']: 
        if collection_name in  pipeline['name']:
            pipeline_ids[pipeline['type']] = pipeline['pipelineId']
        
    return pipeline_ids

def create_pipelines(
    cfg, 
    collection_name: str = "demo",
    data_type: str = "doc",
    metadata_schema: Optional[Dict] = None,
    **kwargs: Any,
) -> dict:
    """Create INGESTION, SEARCH, DELETION pipelines using self.collection_name.

    Args:
        project_id (str): Zilliz Cloud's project ID.
        cluster_id (str): Zilliz Cloud's cluster ID.
        api_key (str=None): Zilliz Cloud's API key. Defaults to None.
        cloud_region (str='gcp-us-west1'): The region of Zilliz Cloud's cluster. Defaults to 'gcp-us-west1'.
        collection_name (str="demo"): A collection name, defaults to 'demo'.
        data_type (str="text"): The data type of pipelines, defaults to "text". Currently only "text" or "doc" are supported.
        metadata_schema (Dict=None): A dictionary of metadata schema, defaults to None. Use metadata name as key and the corresponding data type as value: {'field_name': 'field_type'}.
            Only support the following values as the field type: 'Bool', 'Int8', 'Int16', 'Int32', 'Int64', 'Float', 'Double', 'VarChar'.
        kwargs: optional function parameters to create ingestion & search pipelines.
            - language: The language of documents. Available options: "ENGLISH", "CHINESE".
            - embedding: The embedding service used in both ingestion & search pipeline.
            - reranker: The reranker service used in search function.
            - chunkSize: The chunk size to split a document. Only for doc data.
            - splitBy: The separators to chunking a document. Only for doc data.

    Returns:
        The pipeline ids of created pipelines.

    """
    project_id = cfg.project_id
    cluster_id = cfg.cluster_id
    cloud_region = cfg.cloud_region
    api_key = cfg.api_key
    if data_type == "text":
        ingest_action = "INDEX_TEXT"
        search_action = "SEARCH_TEXT"
    elif data_type == "doc":
        ingest_action = "INDEX_DOC"
        search_action = "SEARCH_DOC_CHUNK"
    else:
        raise Exception("Only text or doc is supported as the data type.")

    params_dict = {}
    additional_params = kwargs or {}

    language = additional_params.pop("language", "ENGLISH")
    embedding = additional_params.pop("embedding", "zilliz/bge-base-en-v1.5")
    reranker = additional_params.pop("reranker", None)
    index_func = {
        "name": "index",
        "action": ingest_action,
        "language": language,
        "embedding": embedding,
    }
    index_func.update(additional_params)
    ingest_functions = [index_func]
    if metadata_schema:
        for k, v in metadata_schema.items():
            preserve_func = {
                "name": f"keep_{k}",
                "action": "PRESERVE",
                "inputField": k,
                "outputField": k,
                "fieldType": v,
            }
            ingest_functions.append(preserve_func)
    params_dict["INGESTION"] = {
        "name": f"{collection_name}_ingestion",
        "projectId": project_id,
        "clusterId": cluster_id,
        "collectionName": collection_name,
        "type": "INGESTION",
        "functions": ingest_functions,
    }

    search_function = {
        "name": "search",
        "action": search_action,
        "clusterId": cluster_id,
        "collectionName": collection_name,
        "embedding": embedding,
    }
    if reranker:
        search_function["reranker"] = reranker
    params_dict["SEARCH"] = {
        "name": f"{collection_name}_search",
        "projectId": project_id,
        "type": "SEARCH",
        "functions": [search_function],
    }

    params_dict["DELETION"] = {
        "name": f"{collection_name}_deletion",
        "type": "DELETION",
        "functions": [
            {
                "name": "purge_by_expression",
                "action": "PURGE_BY_EXPRESSION",
            }
        ],
        "projectId": project_id,
        "clusterId": cluster_id,
        "collectionName": collection_name,
    }

    domain = f"https://controller.api.{cloud_region}.cloud.zilliz.com.cn/v1/pipelines"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    pipeline_ids = {}

    for k, v in params_dict.items():
        response = requests.post(domain, headers=headers, json=v)
        if response.status_code != 200:
            raise RuntimeError(response.text)
        response_dict = response.json()
        if response_dict["code"] != 200:
            raise RuntimeError(response_dict)
        pipeline_ids[k] = response_dict["data"]["pipelineId"]

    return pipeline_ids


class Pipeline():
    def __init__(self, cfg, pipeline_ids):
        self.cfg = cfg
        self.pipeline_ids = pipeline_ids
        self.domain = (
            f"https://controller.api.{self.cfg.cloud_region}.cloud.zilliz.com.cn/v1/pipelines"
        )
        self.headers = {
            "Authorization": f"Bearer {self.cfg.api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def delete_by_doc_name(self, doc_name: str):
        """Delete data by doc name if using the corresponding deletion pipeline."""
        deletion_pipe_id = self.pipeline_ids.get("DELETION")
        deletion_url = f"{self.domain}/{deletion_pipe_id}/run"

        params = {"data": {"doc_name": doc_name}}
        response = requests.post(deletion_url, headers=self.headers, json=params)
        if response.status_code != 200:
            raise RuntimeError(response.text)
        response_dict = response.json()
        if response_dict["code"] != 200:
            raise RuntimeError(response_dict)
        return response_dict["data"]
        
    def insert(self, texts: Union[List[str], str], metadata: Optional[Dict] = None):
        """Insert doc from text with an initialized index using text pipelines."""
        ingest_pipe_id = self.pipeline_ids.get("INGESTION")
        ingestion_url = f"{self.domain}/{ingest_pipe_id}/run"
        if isinstance(texts, str):
            text_list = [texts]
        else:
            text_list = texts
        if metadata is None:
            metadata = {}
        params = {"data": {"text_list": text_list}}
        params["data"].update(metadata)
        response = requests.post(ingestion_url, headers=self.headers, json=params)
        if response.status_code != 200:
            raise RuntimeError(response.text)
        response_dict = response.json()
        if response_dict["code"] != 200:
            raise RuntimeError(response_dict)
        return response_dict["data"]

    def insert_doc_url(self, url: str, metadata: Optional[Dict] = None) -> None:
        """Insert doc from url with an initialized index using doc pipelines."""
        ingest_pipe_id = self.pipeline_ids.get("INGESTION")
        ingestion_url = f"{self.domain}/{ingest_pipe_id}/run"

        if metadata is None:
            metadata = {}
        params = {"data": {"doc_url": url}}
        params["data"].update(metadata)
        response = requests.post(ingestion_url, headers=self.headers, json=params)
        if response.status_code != 200:
            raise RuntimeError(response.text)
        response_dict = response.json()
        if response_dict["code"] != 200:
            raise RuntimeError(response_dict)
        return response_dict["data"]

    def insert_doc_localfile(self, file: str, metadata: Optional[Dict] = None) -> None:
        """Insert doc from url with an initialized index using doc pipelines."""
        if os.path.exists(file) is False:
            raise ValueError(f"{file} doesn't exists.")

        ingest_pipe_id = self.pipeline_ids.get("INGESTION")
        ingestion_url = f"{self.domain}/{ingest_pipe_id}/run_ingestion_with_file"
        headers = dict(self.headers)
        del headers["Accept"]

        if metadata is None:
            metadata = {}
        
        fields = {
            'file': (os.path.basename(file), open(file, 'rb'), 'application/octet-stream'),
            'data': json.dumps(metadata)
        }
        enc = MultipartEncoder(fields=fields)
        headers["Content-Type"] = enc.content_type
        response = requests.post(ingestion_url, headers=headers, data=enc)
        if response.status_code != 200:
            raise RuntimeError(response.text)
        response_dict = response.json()
        if response_dict["code"] != 200:
            raise RuntimeError(response_dict)
        return response_dict["data"]

    def search(self, query_text,  search_params: Dict = {}):

        default_params = {"limit":3,"offset":0,"outputFields":["chunk_id","doc_name"],"filter":"id >= 0"}
        search_pipe_id = self.pipeline_ids.get("SEARCH")
        search_url = f"{self.domain}/{search_pipe_id}/run"

        params = {"data": {"query_text": query_text}, "params": default_params}
        params["params"].update(search_params)
        print(params)
        response = requests.post(search_url, headers=self.headers, json=params)
        if response.status_code != 200:
            raise RuntimeError(response.text)
        response_dict = response.json()
        if response_dict["code"] != 200:
            raise RuntimeError(response_dict)
        return response_dict["data"]

if __name__ == '__main__':
    config = ZillizConfig()
    pipeline_ids = list_pipelines(config, "demo")
    if len(pipeline_ids) != 0:
        ppl = Pipeline(config, pipeline_ids)
    else:
        ppl = create_pipelines(config, "demo")
    
    if os.path.exists("ml_corpus.txt") is False:
        print("请先生成ml_corpus.txt")
        exit()
    
    ppl.insert_doc_localfile("ml_corpus.txt")

    
