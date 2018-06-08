from celery.task import task
from subprocess import call,STDOUT
import requests,os, json
from okapi import getOkapiData, postOkapiData, okapiHeaders, getQueueConfig
from okapi import deleteAllOkapi
from cyberapi import getSampleData

@task()
def loadFixedDueDateSchedules(data=None,user=None,tenant='diku',tag='default'):
    """
    kwargs:
        Not required - defaults - override for custom control
        data= <type list> Fixed Due Date Schedules
        user= queueconfig.adminuser (file located next to celeryconfig)
               {"username":"< enter >","password":"< enter >"}
        tenant= 'diku' - override with string of tenant
        tag = Inside sample data you can add a tag to filter the appropriate
              Sample data.
    """
    if not user:
        user = getQueueConfig()['okapiAdmin']
    if data:
        data=json.loads(data)
    else:
        #Get sample data no auth needed for get operations
        query ='{{"filter":{{"tag":"{0}"}},"projection":{{"_id":0,"tag":0}}}}'.format(tag)
        data=getSampleData("catalog","fixedDueDateSchedules",query=query)
    headers = okapiHeaders(user['username'],user['password'],tenant)
    path="fixed-due-date-schedule-storage/fixed-due-date-schedules"
    added = errors = 0
    err=[]
    #post FixedDueDateSchedules to Okapi
    for fdds in data['results']:
        try:
            postOkapiData(fdds,path,headers)
            added +=1
        except Exception as inst:
            errors +=1
            err.append(str(inst).replace('"',''))
    return {"Inserted":added,"Errors":{"count":errors,"errors":err}}

@task()
def deleteFixedDueDateSchedules(tenant='diku'):
    """
    Delete all FixedDueDateSchedules.
    """
    user = getQueueConfig()['okapiAdmin']
    headers = okapiHeaders(user['username'],user['password'],tenant)
    path="fixed-due-date-schedule-storage/fixed-due-date-schedules"
    key = 'fixedDueDateSchedules'
    return deleteAllOkapi(path,key, headers)
