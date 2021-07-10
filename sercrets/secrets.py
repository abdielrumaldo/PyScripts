import requests
import boto3
import datetime
import base64
import hashlib
import hmac
import json
import re


restapi_id = 'f1okrx23ab'
region = 'us-east-1'
stage_name = ''
base_url = 'https://f1okrx23ab.execute-api.us-east-1.amazonaws.com/test'

def check_token(event):
    data = event['body']
    access_token=data['access_token']
    refresh_token=data['refresh_token']
    renew=True
    now = datetime.now()
    dec_access_token=jwt.get_unverified_claims(access_token)
    un=dec_access_token['username']
    clientID=dec_access_token['client_id']
    if now > datetime.fromtimestamp(dec_access_token['exp']):
        expired = True
        if renew:
            u=renew_access_token(refresh_token,clientID,un)
    else:
        expired = False
        return {"body":{"id_token":data['id_token'],"access_token":data['access_token'],"refresh_token":data['refresh_token']},"headers": {}, "statusCode": 200,"isBase64Encoded":"false","expired":expired}
    return {"body":{"id_token":u['id_token'],"access_token":u['access_token'],"refresh_token":u['refresh_token']},"headers": {}, "statusCode": 200,"isBase64Encoded":"false","expired":expired}

def renew_access_token(refresh_token,clientID,un):
    client = boto3.client('cognito-idp','us-east-1')
    try:
        ref_response = client.initiate_auth(
        ClientId='5atnmuafq71rpe3bjal3hm0l2r',
        AuthFlow='REFRESH_TOKEN_AUTH',
        AuthParameters={
            'REFRESH_TOKEN':refresh_token,
            'SECRET_HASH': gethmacdigest(clientID, un)
            }
        )
        ref=ref_response['AuthenticationResult']
    except client.exceptions.NotAuthorizedException as e:
        return {"body":{"Error":"Please try again"},"headers": {}, "statusCode": 401,"isBase64Encoded":"false"}
    return {"id_token":ref['IdToken'],"access_token":ref['AccessToken'],"refresh_token":refresh_token}
    
def log_out(event):
    client = boto3.client('cognito-idp','us-east-1')
    data = event['body']
    try:
        response = client.global_sign_out(
                    AccessToken=data['access_token']
                    )
    except ForceChangePasswordException:
        print('Something went wrong')
    return {"body":"Successfully logged out!","headers": {}, "statusCode": 200,"isBase64Encoded":"false"}

def gethmacdigest(clientid, username):
        message = username + clientid
        dig = hmac.new(b"1oam9u25gjit0dfrm9v3un64ecea1utr8crdreqg06cl0pc26nih", msg=message.encode('UTF-8'), digestmod=hashlib.sha256).digest()   
        return base64.b64encode(dig).decode()
   
def get_token(event):
    client = boto3.client('cognito-idp','us-east-1')
    ClientId='5atnmuafq71rpe3bjal3hm0l2r'
    data = event['body']
    un=data['username']
    p=data['password']
    try:
        response = client.initiate_auth(
            ClientId='5atnmuafq71rpe3bjal3hm0l2r',
            AuthFlow='USER_PASSWORD_AUTH',
            AuthParameters={
                'USERNAME': un,
                'PASSWORD': p,
                'SECRET_HASH': gethmacdigest(ClientId, un)
                }
            )
        u=response['AuthenticationResult']
    except client.exceptions.NotAuthorizedException as e:
        print(e)
        return {"body":{"Error":"Please try again"},"headers": {}, "statusCode": 401,"isBase64Encoded":"false"}
    return {"body":{"id_token":u['IdToken'],"access_token":u['AccessToken'],"refresh_token":u['RefreshToken']},"headers": {}, "statusCode": 200,"isBase64Encoded":"false"}


# packet={'body':{    
#         "username": "arumaldo",
#         "password": "Texas512!"
#      }}

# token_data=get_token(packet)
# print(token_data)
# # Extract the authorization header "id_token"
# header={'Authorization': token_data['body']['id_token']}

# data = {'burn': '1', 'how': [295, 296], 'siteName': 'wakeforest', 'subjectID': '02-1026', 'user': 'rumaldo'}
# data2= { 'body': { 'burn': '1','how': [295, 296], 'siteName': 'wakeforest', 'subjectID': '02-1026', 'user': 'rumaldo'} }

# transformed_data = json.dumps(data)
# print(f'Headers: \n {header} \n\nData: \n {transformed_data}')

# job_request_json = requests.post(f'{base_url}/createjobs', data=transformed_data, headers=header)
# job_json = job_request_json.json()
# print(job_json)

# headers = {"Content-Type": "application/x-amz-json-1.1", "X-Amz-Target":"AWSCognitoIdentityProviderService.GetUser", "Content-Length" : f'{1162 //len(token_data["body"]["access_token"].encode("utf-16"))}'}
# data = json.dumps({'AccessToken': token_data['body']['access_token']})
# attributes = requests.post("https://cognito-idp.us-east-1.amazonaws.com",headers=headers, data=data)
# print(attributes.json())

# class X:
#     def __init__(self):
#         self.x = 10

#     def second(self):
#         self.x = 15

# p1 = X()
# print(p1.x)

data = "arn:aws:sagemaker:us-east-1:921329379223:labeling-job/wakeforest-subject02-1024burn2id292008"

full_id = data.split('/')[1]
print(full_id)
id = full_id.split("id")[1]
print(id)

subject_id = re.search('(?<=subject).+(?=burn)', data)
wound_number = re.search('(?<=burn).+(?=id)', data).group(0)
job_id = re.search('(?<=id).+', data).group(0)

print(subject_id.group(0))
print(wound_number)


