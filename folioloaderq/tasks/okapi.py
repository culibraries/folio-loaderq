import requests, os,json

host_url =os.getenv('HOSTURL',"http://okapihost")

def getOkapiData(path,headers):
    url="{0}:9130/{1}".format(host_url,path)
    req=requests.get(url,headers=headers)
    if req.status_code >=400:
        raise Exception("Okapi API Error: {0}".format(req.text))
    return req.json()

def postOkapiData(data,path,headers):
    url="{0}:9130/{1}".format(host_url,path)
    req=requests.post(url,json.dumps(data),headers=headers)
    if req.status_code >=400:
        raise Exception("Okapi API Error: {0}".format(req.text))
    return req.json()

def deleteOkapiData(id,path,headers):
    url="{0}:9130/{1}/{2}".format(host_url,path,id)
    req=requests.delete(url,headers=headers)
    if req.status_code >=400:
        raise Exception("Okapi API Error: {0}".format(req.text))
    return req.json()

def deleteAllOkapi(path,key, headers):
    """
    Delete all Records.
    args:
        path <type string> okapi path url
        key return values key
        headers for okapi to validate user
    """
    data = getOkapiData(path,headers)
    deleted = errors = 0
    err=[]
    for rec in data[key]:
        try:
            deleteOkapiData(rec["id"],path,headers)
            deleted +=1
        except Exception as inst:
            errors +=1
            err.append(str(inst).replace('"',''))
    return {"Deleted":deleted,"Errors":{"count":errors,"errors":err}}



def getQueueConfig(configfile='queueconfig.json'):
    with open(configfile,'r') as f1:
        return json.loads(f1.read())

def okapiHeaders(username,password,tenant='diku'):
    url = "{0}:9130/authn/login".format(host_url)
    data={"username": username, "password": password}
    headers={'Content-Type':'application/json','X-Okapi-Tenant':tenant}

    req = requests.post(url,data=json.dumps(data),headers=headers)
    if req.status_code >=400:
        raise Exception("Authentication failed {0}".format(req.text))
    headers['x-okapi-token']=req.headers['x-okapi-token']
    return headers
