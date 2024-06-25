
import boto3
from botocore.exceptions import ClientError
import requests
import json
import os 

def assume_role():
    '''
    Creates initial session with specified profile - that user has no other permissions
    Allows uses to assume to temporary role that has access to S3 - everything pre configured in AWS Console
    '''
    try:
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
       
    # Create a boto3 session
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name='eu-west-2'
            )
        
        # create sts client using the session object
        sts_client = session.client('sts', region_name='eu-west-2')
        # assume role with correct permissions
        assumed_role_object = sts_client.assume_role(RoleArn='arn:aws:iam::058264124972:role/S3AccessRole',
                RoleSessionName='s3AccessRole')
        temporary_credentials = assumed_role_object['Credentials']
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AccessDenied':
            print("Access denied. Check your permissions.")
        else:
            print(f"Unexpected error: {e}")
        return None
        

    # create new session with temporary credentials
    try:
        session = boto3.Session(
            aws_access_key_id = temporary_credentials['AccessKeyId'],
            aws_secret_access_key = temporary_credentials['SecretAccessKey'],
            aws_session_token = temporary_credentials['SessionToken'])

        # return new session with temporary credentials
        return session
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AccessDenied':
            print("Creating temporary session failed. Check your permissions.")
        else:
            print(f"Unexpected error: {e}")
        return None        

def max_existing_IDs(S3client, bucket):
    bucket_contents = S3client.list_objects_v2(Bucket = bucket)
    max_id = max([n['Key'][0:n['Key'].find('_')] for n in bucket_contents['Contents']])
    return max_id

def get_pokemon(next_id):

    id = int(next_id)+1
    APIurl = f'https://pokeapi.co/api/v2/pokemon/{id}/'

    try:
        response = requests.get(url = APIurl,timeout = 1)
        response.raise_for_status() 
    except requests.exceptions.HTTPError as errh: 
        print("HTTP Error") 
        print(errh.args[0]) 
    except requests.exceptions.ReadTimeout as errrt: 
        print("Time out") 
    except requests.exceptions.ConnectionError as conerr: 
        print("Connection error") 
    except requests.exceptions.RequestException as errex: 
        print("Exception request") 

    poke_json = response.json()

    poke_json = {k: poke_json[k] for k in {'id','name','abilities'}}

    pokemon_name = poke_json['name']
    pokemon_id = poke_json['id']

    return poke_json
   
def upload_to_bucket(S3client, json_file ,file_name):
    try:
        S3client.put_object(Bucket='pokemon-api-json', 
                             Key=f'{file_name}.json', 
                             Body=json.dumps(json_file))
        print(f'{file_name}.json uploaded to S3')
    except ClientError as e:
        print(e)


def main():

    # start initial AWS session    
    temp_session_new = assume_role()
    
    # create new client using temp session
    s3_client = temp_session_new.client('s3',region_name='eu-west-2')

    # find max ID of last JSON in S3 bucket
    maxid = max_existing_IDs(s3_client,'pokemon-api-json')

    # download pokemon json
    json_pokemon = get_pokemon(maxid)

    # create name for the new JSON
    json_name = f'''{json_pokemon['id']}_{json_pokemon['name']}'''

    # upload it to S3
    upload_to_bucket(s3_client, json_pokemon, json_name)

if __name__ == "__main__":
    main()