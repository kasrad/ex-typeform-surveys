"__author__ = 'Radim Kasparek kasrad'"
"__credits__ = 'Keboola Drak"
"__component__ = 'Typeform Extractor'"

"""
Python 3 environment 
"""


import pandas as pd
import requests
from pandas.io.json import json_normalize
import numpy as np
from keboola import docker
from datetime import datetime, timedelta
import logging

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")

cfg = docker.Config('/data/')
token = cfg.get_parameters()['#token']
form_id = cfg.get_parameters()['form_id']
dayspan = cfg.get_parameters()['dayspan']

since = (datetime.utcnow() - timedelta(days = int(dayspan)))\
        .isoformat()
headers = {'Authorization': 'bearer %s' % token}
params = {'since': since
          }


# url and the request
url = 'https://api.typeform.com/forms/' + form_id + '/responses'
resp = requests.get(url=url, params=params, headers=headers)
no_n_responses = len(resp.json()['items'])
logging.info('The number of new responses is: ' + str(no_n_responses))

# flattening the response
results_df = pd.DataFrame(np.zeros((0, 0)))
if no_n_responses > 0:
    for i in range(len(resp.json()['items'])):
        logging.info('Retrieving response #' + str(i))
        # id of the applicant + time_submitted
        id_applicant = resp.json()['items'][i]['metadata']['referer']
        time_submitted = resp.json()['items'][i]['submitted_at']

        # create df from flattened json
        response_df = json_normalize(resp.json()['items'][i]['answers'])
        resp_df_cols = response_df.columns

        # all the columns that could be of interest
        all_cols_needed = ['choices.labels', 'email', 'number', 'text',
                           'boolean', 'field.id', 'url', 'choice.label',
                           'file_url', 'date', 'payment']

        # add the columns to those responses that do not contain them
        for col_name in resp_df_cols:
            response_df[col_name] = '' if (col_name not in all_cols_needed) \
                                    else response_df[col_name]
        
        # take only the columns of interest, switch NaNs with '' for later concat
        answers = response_df\
            .loc[:, all_cols_needed]\
            .fillna('')

        # cast the columns as strings
        answers['choices.labels'] = answers['choices.labels']\
            .astype(str)
        answers['number'] = answers['number']\
            .astype(str)
        answers['boolean'] = answers['boolean']\
            .astype(str)

        # concat the values
        answers['ans_concat'] = answers[['text', 'email', 'number',
                                         'choices.labels', 'boolean',
                                         'url']]\
            .apply(lambda x: ''.join(x), axis=1)

        # id of the question + value
        results_df_tmp = answers[['field.id', 'ans_concat']]
        results_df_tmp = results_df_tmp.set_index('field.id').T
        results_df_tmp['id'] = [id_applicant]
        results_df_tmp['time_submitted'] = [time_submitted]

        # in the first run create the df, in the following just append
        if results_df.empty:
            results_df = results_df_tmp
        else:
            results_df = results_df.append(results_df_tmp)

        logging.info('Response #%s retrieved' % str(i))

    # write the results
    results_df.to_csv('/data/out/tables/answers_applicants.csv', index=False)
    
else:
    logging.info('No new responses to fetch.')