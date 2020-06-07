"""run with: poetry run python get_survey.py"""
import os
import sys
import json
import argparse
from pathlib import Path

import dotenv
import requests

MAX_REQUESTS = 10


parser = argparse.ArgumentParser()
parser.add_argument('survey_id', help='the survey id', type=int)
parser.add_argument('-e', '--endpoint', help='API endpoint to hit',
                    metavar='STRING', choices=['details', 'responses'],
                    default='details')
parser.add_argument('-t', '--token', help='the access token',
                    metavar='STRING')
parser.add_argument('-o', '--output', help='write response to location',
                    metavar='FILE')

args = parser.parse_args()

token = args.token
if token is None:
    # try load from .env file
    dotenv.load_dotenv()
    token = os.getenv("ACCESS_TOKEN")
    if token is None:
        sys.exit("get_survey_details.py: error: can't find access token, "
                 "either pass argument --token or add ACCESS_TOKEN to .env.")

host = 'https://api.surveymonkey.com/v3/surveys'
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json',
}

results = []

if args.endpoint == 'details':
    url = f'{host}/{args.survey_id}/details'
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    results.append(response.json())

if args.endpoint == 'responses':
    url = f'{host}/{args.survey_id}/responses/bulk/?per_page=10'
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    response = response.json()
    results.append(response)
    r = 1
    while 'next' in response['links'] and r < MAX_REQUESTS:
        url = response['links']['next']
        response = requests.get(url, headers=headers).json()
        results.append(response)
        r += 1

if args.output:
    out = Path(args.output)
    if len(results) > 1:
        out.mkdir(parents=True, exist_ok=True)
        for i, response in enumerate(results):
            fname = f'responses-{i:05}.json'
            with open(Path(out, fname), 'w') as f:
                json.dump(response, f)
    else:
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, 'w') as f:
            json.dump(results[0], f)
else:
    print(results[0])
