
import requests
import json
import pprint

host = "http://vm-75222.lal.in2p3.fr:21111"

headers = {'Content-Type': 'application/json'}

data = {'kind': 'pyspark'}
r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
print(r.json())

data = {'code': '1 + 1'}
r = requests.post(host + '/statements', data=json.dumps(data), headers=headers)
# pprint.pprint(r.json())

requests.delete(host + '/session/1', headers=headers)


