import requests
import json
import csv
import hashlib
import time

customerId = "/"
listId = "/"

def get_access_token():
    url = 'https://www.googleapis.com/oauth2/v4/token?grant_type=refresh_token&client_id=...U'

    response = requests.post(url)
    if response.status_code == 200:
        response_data = response.json()
        access_token = response_data.get('access_token')
        return access_token
    else:
        print('Error:', response.status_code)
        return None

access_token = get_access_token()
if access_token:
    print('Access Token:', access_token)
else:
    exit()

headers = {
    'Content-Type': 'application/json',
    'developer-token' : '.',
    'login-customer-id' : '.',
    'Authorization' : 'Bearer ' + access_token
}


url = f"https://googleads.googleapis.com/v14/customers/{customerId}/offlineUserDataJobs:create"

payload = {
    "job": {
        "customerMatchUserListMetadata": {
            "userList": f"customers/{customerId}/userLists/{listId}"
        },
        "resourceName": f"customers/{customerId}/userLists/{listId}",
        "type": "CUSTOMER_MATCH_USER_LIST"
    },
    "validateOnly": False,
    "enableMatchRateRangePreview": True
}

response = requests.post(url, data=json.dumps(payload), headers=headers)
response_body = response.json()

resourceName = response_body.get("resourceName", None)

print(response.status_code)
print(resourceName)



url_create_operations = f"https://googleads.googleapis.com/v14/{resourceName}:addOperations"

csv_file = 'extract.csv'
email_hashes = []

with open(csv_file, 'r') as file:
    csv_reader = csv.reader(file)
    next(csv_reader)
    for row in csv_reader:
        email = row[0].strip() 
        hashed_email = hashlib.sha256(email.encode()).hexdigest()
        email_hashes.append(hashed_email)

sets = [email_hashes[i:i+100] for i in range(0, len(email_hashes), 100)]
for email_set in sets:
    operations = []
    for i in range(0, len(email_set), 20):
        chunk = email_set[i:i+20]
        identifiers = []
        for email_hash in chunk:
            identifier = {"hashedEmail": email_hash}
            identifiers.append(identifier)

        operation = {"create": {"userIdentifiers": identifiers}}
        operations.append(operation)

    json_body = {
        "validateOnly": False,
        "enableWarnings": False,
        "enablePartialFailure": True,
        "operations": operations
    }

    response_create_operations = requests.post(url_create_operations, data=json.dumps(json_body), headers=headers)

    print(response_create_operations.status_code)
    print(response_create_operations.text)

    json_body = None
    # time.sleep(4)



url = f"https://googleads.googleapis.com/v14/{resourceName}:run"
response = requests.post(url, headers=headers)

print(response.status_code)
print(response.json())
