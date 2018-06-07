import requests, os,json

host_url =os.getenv('HOSTURL',"http://okapihost")

def okapiHeaders(username,password,tenant='diku'):
    url = "{0}:9130/authn/login".format(host_url)
    data={"username": username, "password": password}
    headers={'Content-Type':'application/json','X-Okapi-Tenant':tenant}

    req = requests.post(url,data=json.dumps(data),headers=headers)
    if req.status_code >=400:
        raise Exception("Authentication failed {0}".format(req.text))
    headers = headers['x-okapi-token']=req.headers['x-okapi-token']
    return headers
