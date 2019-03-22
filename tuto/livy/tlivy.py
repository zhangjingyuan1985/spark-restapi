
import requests
import json
import pprint

host = "http://vm-75222.lal.in2p3.fr:21111"

def livy_get_sessions():
    ids=[]
    r = requests.get(host + '/sessions')
    answer = r.json()
    for session in answer['sessions']:
        ids.append(session['id'])

    return ids

def livy_create_session():
    headers = {'Content-Type': 'application/json'}
    data = {'kind': 'pyspark'}
    r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    answer = r.json()
    id = int(answer['id'])
    return int(answer['id'])

def livy_delete_session(id):
    r = requests.delete(host + '/sessions/{}'.format(id))


def main():
    print('Show all session ids')
    print(' - '.join([str(id) for id in livy_get_sessions()]))

    print('Create a new session')
    id = livy_create_session()
    print(id)

    print('Delete it')
    print(livy_delete_session(id))

    print('Show all session ids')
    print(' - '.join([str(id) for id in livy_get_sessions()]))

    """
    data = {'code': '1 + 1'}
    r = requests.post(host + '/statements', data=json.dumps(data), headers=headers)
    # pprint.pprint(r.json())
    
    """


if __name__ == "__main__":
    main()