from celery.task import task
import requests, os,json
from subprocess import call,check_output,STDOUT

folioDataLoader_url =os.getenv('folioDataLoader_url',"http://dataloader_folio")

@task()
def loadMarcRules(json=None):
    headers={'Content-Type':'application/octet-stream','X-Okapi-Tenant':'diku'}
    if json:
        filename = 'rules.json'
        with open('rules.json','w') as f1:
            f1.write(json)
    else:
        filename='rules-default.json'

    url = "{0}:8081/load/marc-rules".format(folioDataLoader_url)
    command = ['curl','-X POST',"-H 'x-okapi-tenant:diku'",
                "-H 'Content-Type:application/octet-stream'",
                "-d @{0}".format(filename),url]
    result=check_output(command)
    if result =='':
        return {"status":True,"message":"Rules uploaded","error":''}
    return {"status":False,"message":"Rules not uploaded","error":result}



"""
    url = "{0}:8081/load/marc-rules".format(folioDataLoader_url)
    req = requests.post(url, files=files,headers=headers)
    if req.status_code < 400:
        return {"status":True,"message":"Rules uploaded","error":''}
    return {"status":False,"message":"Rules not uploaded","error":req.text}

curl -X POST -H 'x-okapi-tenant:diku' -H 'Content-Type:application/octet-stream' -d @rules.json    http://localhost:8081/load/marc-rules

"""
