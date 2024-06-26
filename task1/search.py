from pipeline import *

if __name__ == '__main__':
    config = ZillizConfig(project_id, cluster_id, api_key)
    pipeline_ids = list_pipelines(config, "demo")
    if len(pipeline_ids) != 0:
        ppl = Pipeline(config, pipeline_ids)
    else:
        ppl = create_pipelines(config, "demo")
    
    contexts = ppl.search("What is ResNet", {"limit":10})  
    print(contexts)
