# ex-typeform-surveys

Keboola Connection docker app for fetching responses from Typeform Responses API. Available under `radim-kasparek.ex-typeform-surveys`

## Functionality
This component allows KBC to fetch responses to Typeform questionnaires.

## Parameters

There are currently three options in the UI:
- Personal access token - https://developer.typeform.com/get-started/personal-access-token/
- Id of the questionnaire - can be found in the URL of the questionnaire
- How many days back you want to go - denotes the start of the time interval you fetch responses from.

## Outcome
The column names in the resulting table are the question IDs.
