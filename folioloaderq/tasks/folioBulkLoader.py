from celery.task import task
import requests, os,json

folioDataLoader_url =os.getenv('folioDataLoader_url',"http://dataloader_folio")

@task()
def loadMarcRules(json=None):
    headers = headers={'Content-Type':'application/octet-stream','X-Okapi-Tenant':'diku'}
    if json:
        files = {'file': ('rules.json', json)}
    else:
        with open('rules.json','rb') as f1
            files = {'file': ('rules.json', f1.read())}
    url = "{0}:8081/load/marc-rules".format(folioDataLoader_url)
    req = requests.post(url, files=files)
    if req.status_code < 400:
        return {"status":True,"message":"Rules uploaded","error":''}
    return {"status":False,"message":"Rules not uploaded","error":req.text}

"""
curl -X POST -H 'x-okapi-tenant:diku' -H 'Content-Type:application/octet-stream' -d @rules.json    http://localhost:8081/load/marc-rules

"""
