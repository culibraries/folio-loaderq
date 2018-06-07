from celery.task import task
from subprocess import call,STDOUT
import requests,os, json
from okapi import okapiHeaders

host_url =os.getenv('OKAPIURL',"http://okapihost")

@task()
def loadFixedDueDateSchedules(data=None,user=None,tenant='diku'):
    """
    kwargs:
        Not required - defaults - override for custom control
        data= <type list> Fixed Due Date Schedules
        user= queueconfig.adminuser (file located next to celeryconfig)
               {"username":"< enter >","password":"< enter >"}
        tenant= 'diku' - override with string of tenant
    """
    if not user:
        user = getQueueConfig()['okapiAdmin']

    if data:
        data=json.loads(data)
    else:
        #Get sample data no auth needed for get operations
        data=getCybercomData("catalog","fixedDueDateSchedules")
    print(user)
    headers = okapiHeaders(user['username'],user['password'],tenant)
    path="fixed-due-date-schedule-storage/fixed-due-date-schedules"
    added=0
    errors=0
    err=[]
    #post FixedDueDateSchedules to Okapi
    for fdds in data['results']:
        try:
            postOkapiData(fdds,path,headers)
            addded +=1
        except Exception as inst:
            errors +=1
            err.append(str(inst))
    return {"Inserted":added,"Errors":{"count":errors,"errors":err}}

def postOkapiData(data,path,headers):
    url="{0}:9130/{1}".format(host_url,path)
    req=requests.post(url,json.dumps(data),headers=headers)
    if req.status_code >=400:
        raise Exception("Okapi API Error: {0}".format(req.text))
    return req.json()
def getCybercomData(database,collection,query='{}',page_size=0):
    url="{0}:9888/api/data_store/data/{1}/{2}/.json?query={3}&page_size={4}"
    url= url.format(host_url,database,collection,query,page_size)
    req=requests.get(url)
    if req.status_code >=400:
        raise Exception("Cybercom API Error: {0}".format(req.text))
    return req.json()

def getQueueConfig():
    with open('queueconfig.json','r') as f1:
        return json.loads(f1.read())
