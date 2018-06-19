from celery.task import task
import requests, os,json
#from subprocess import call,check_output,STDOUT
from commandline import commandLineExec

folioDataLoader_url =os.getenv('folioDataLoader_url',"http://dataloader_folio")


@task()
def loadMarcRules(marc_rules=None):
    """
    Load MARC Rules for the bulk data bulk data loader.

    Signature:
        loadMarcRules(json=None)
    kwargs:
        marc_rules - json - default(https://github.com/folio-org/mod-data-loader/blob/master/ramls/rules.json)
    """
    if marc_rules:
        filename = 'rules.json'
        with open('rules.json','w') as f1:
            f1.write(marc_rules)
    else:
        filename='rulesdefault.json'

    url = "{0}:8081/load/marc-rules".format(folioDataLoader_url)
    command = ['curl','-X POST',"-H", "x-okapi-tenant:diku",
                "-H", "Content-Type:application/octet-stream",
                "-d", "@{0}".format(filename),url]
    print(command)
    try:
        result=commandLineExec(command)
    except:
        raise

    return {"status":True,"message":"Rules uploaded","output": result}



"""
    headers={'Content-Type':'application/octet-stream','X-Okapi-Tenant':'diku'}
    json_folder= "/vagrant/{0}"
    url = "{0}:8081/load/marc-rules".format(folioDataLoader_url)
    req = requests.post(url, files=files,headers=headers)
    if req.status_code < 400:
        return {"status":True,"message":"Rules uploaded","error":''}
    return {"status":False,"message":"Rules not uploaded","error":req.text}

    curl -X POST -H 'x-okapi-tenant:diku' -H 'Content-Type:application/octet-stream' -d @rules.json    http://localhost:8081/load/marc-rules
"""
