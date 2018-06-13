from celery.task import task
from celery import signature, group
from reference import loadFixedDueDateSchedules, loadLoanPolicies, deleteReferenceCirculationData
import requests,os, json



@task()
def circulationReferenceWorkflow(tag='default',tenant='diku'):
    """
    Workflow deletes and loads Loan Policy and Fixed Due DateSchedules associated with tag. 

    Signature:
        circulationReferenceWorkflow(tag='default',tenant='diku')
    kwargs:
        tag: Identifies set of data stored in catalog
        tenant: diku

    """
    queuename = circulationReferenceWorkflow.request.delivery_info['routing_key']
    workflow = (deleteReferenceCirculationData.s(tenant=tenant).set(queue=queuename) |
                loadLoanPolicies.si(tenant=tenant,tag=tag).set(queue=queuename) |
                loadFixedDueDateSchedules.si(tenant=tenant,tag=tag).set(queue=queuename))()
    return "Succefully submitted circulation Reference Workflow(tag='{0}',tenant='{1}')".format(tag,tenant)
