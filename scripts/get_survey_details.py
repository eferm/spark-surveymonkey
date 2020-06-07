"""run with: poetry run python get_survey_details.py"""
import os
import sys
import json
import argparse

import dotenv
import requests


parser = argparse.ArgumentParser()
parser.add_argument('survey_id', help='the survey id', type=int)
parser.add_argument('-o', '--output', help='write response to file', metavar='FILE')
parser.add_argument('--access-token', help='the access token', metavar='STRING')

args = parser.parse_args()
print(args)

token = args.access_token
if token is None:
    # try load from .env file
    dotenv.load_dotenv()
    token = os.getenv("ACCESS_TOKEN")
    if token is None:
        sys.exit("get_survey_details.py: error: unable to locate access token, "
                 "either add ACCESS_TOKEN to .env or pass argument --access-token")

url = 'https://api.surveymonkey.com/v3/surveys'
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json',
}

response = requests.get(f'{url}/{args.survey_id}/details', headers=headers)

if args.output:
    with open(args.output, 'w') as f:
        json.dump(response.json(), f)
else:
    print(response.json())
