import pandas as pd
import requests
from pandas.io.json import json_normalize
import numpy as np
from keboola import docker


cfg = docker.Config('/data/')
token = cfg.get_parameters()['#token']
form_id = cfg.get_parameters()['form_id']

#the timestamp for since should be provided in ISO 8601 format, UTC time
#e.g. '2017-08-09T00:00:00'  August 9, 2017 at 12:00 a.m. UTC
since = cfg.get_parameters()['since']


headers = {'Authorization': 'bearer %s' % token}
params = {'since': since
          }


#url and the request
url = 'https://api.typeform.com/forms/' + form_id + '/responses'
resp = requests.get(url=url, params=params, headers=headers)
print('The number of new responses is: ' + str(len(resp.json()['items'])))

#flattening the response
results_df = pd.DataFrame(np.zeros((0, 0)))
for i in range(len(resp.json()['items'])):
    print('Retrieving response #' + str(i))
    answers = json_normalize(resp.json()['items'][i]['answers'])\
        .loc[:, ['choices.labels', 'email', 'number',
                 'text', 'boolean', 'field.id']]\
        .fillna('')

    answers['choices.labels'] = answers['choices.labels']\
        .astype(str)

    answers['number'] = answers['number']\
        .astype(str)
    answers['boolean'] = answers['boolean']\
        .astype(str)

    answers['ans_concat'] = answers[['text', 'email', 'number',
                                     'choices.labels', 'boolean']]\
        .apply(lambda x: ''.join(x), axis=1)

    results_df_tmp = answers[['field.id', 'ans_concat']]

    results_df_tmp = results_df_tmp.set_index('field.id').T

    if results_df.empty:
        results_df = results_df_tmp
    else:
        results_df = results_df.append(results_df_tmp)

    print('Response #%s retrieved' % str(i))




#write the results
results_df.to_csv('/data/out/tables/answers_applicants.csv', index=False)
