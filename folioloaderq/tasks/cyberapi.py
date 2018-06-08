import requests,json

host_url =os.getenv('HOSTURL',"http://okapihost")

def getSampleData(database,collection,query='{}',page_size=0):
    url="{0}:9888/api/data_store/data/{1}/{2}/.json?query={3}&page_size={4}"
    url= url.format(host_url,database,collection,query,page_size)
    req=requests.get(url)
    if req.status_code >=400:
        raise Exception("Cybercom API Error: {0}".format(req.text))
    return req.json()
