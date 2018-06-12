from celery.task import task
from subprocess import call,STDOUT
import requests,os, json
from okapi import getOkapiData, postOkapiData, okapiHeaders, getQueueConfig
from okapi import deleteAllOkapi, loadOkapiData
from cyberapi import getSampleData



@task()
def loadFixedDueDateSchedules(data=None,user=None,tenant='diku',tag='default'):
    """
    signature:

        loadFixedDueDateSchedules(data=None,user=None,tenant='diku',tag='default')

    kwargs:(Not required - defaults and code supplied values if None)
        data= <type list> (Check catalog/fixedDueDateSchedules for sample records)
        user= queueconfig.adminuser (file located next to celeryconfig)
               {"username":"< enter >","password":"< enter >"}
        tenant= default 'diku'
        tag = Query specific tag for sample input data. Please check catalog for
              appropriate tags.(default is the default set)
    """
    if data:
        data=json.loads(data)
    else:
        #Get sample data no auth needed for get operations
        query ='{{"filter":{{"tag":"{0}"}},"projection":{{"_id":0,"tag":0}}}}'.format(tag)
        data=getSampleData("catalog","fixedDueDateSchedules",query=query)['results']
    path="fixed-due-date-schedule-storage/fixed-due-date-schedules"
    return loadOkapiData(data,path,user=user,tenant=tenant)

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
