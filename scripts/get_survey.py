"""run with: poetry run python get_survey.py"""
import os
import sys
import json
import pathlib
import argparse

import dotenv
import requests


parser = argparse.ArgumentParser()
parser.add_argument('survey_id', help='the survey id', type=int)
parser.add_argument('-e', '--endpoint', help='API endpoint to hit',
                    metavar='STRING', choices=['details', 'responses'],
                    default='details')
parser.add_argument('-t', '--token', help='the access token', metavar='STRING')
parser.add_argument('-o', '--output', help='write response to file',
                    metavar='FILE')

args = parser.parse_args()

token = args.token
if token is None:
    # try load from .env file
    dotenv.load_dotenv()
    token = os.getenv("ACCESS_TOKEN")
    if token is None:
        sys.exit("get_survey_details.py: error: unable to locate access token, "
                 "either add ACCESS_TOKEN to .env or pass argument --token")

host = 'https://api.surveymonkey.com/v3/surveys'
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json',
}

if args.endpoint == 'details':
    url = f'{host}/{args.survey_id}/details'
    response = requests.get(url, headers=headers)
elif args.endpoint == 'responses':
    url = f'{host}/{args.survey_id}/responses/bulk/?per_page=100'
    response = requests.get(url, headers=headers)
else:
    raise ValueError(f'endpoint not supported: {args.endpoint}')

if args.output:
    pathlib.Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, 'w') as f:
        json.dump(response.json(), f)
else:
    print(response.json())
