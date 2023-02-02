import numpy as np
import pandas as pd
from datetime import timedelta, datetime
from google.oauth2 import service_account
from apiclient.discovery import build
import json


def format_summary(response):
    try:
        # create row index
        try: 
            row_index_names = response['reports'][0]['columnHeader']['dimensions']
            row_index = [ element['dimensions'] for element in response['reports'][0]['data']['rows'] ]
            row_index_named = pd.MultiIndex.from_arrays(np.transpose(np.array(row_index)), 
                                                        names = np.array(row_index_names))
        except:
            row_index_named = None
        
        # extract column names
        summary_column_names = [item['name'] for item in response['reports'][0]
                                ['columnHeader']['metricHeader']['metricHeaderEntries']]
    
        # extract table values
        summary_values = [element['metrics'][0]['values'] for element in response['reports'][0]['data']['rows']]
    
        # combine. I used type 'float' because default is object, and as far as I know, all values are numeric
        df = pd.DataFrame(data = np.array(summary_values), 
                          index = row_index_named, 
                          columns = summary_column_names).astype('float')
    
    except:
        df = pd.DataFrame()
        
    return df

def format_pivot(response):
    try:
        # extract table values
        pivot_values = [item['metrics'][0]['pivotValueRegions'][0]['values'] for item in response['reports'][0]
                        ['data']['rows']]
        
        # create column index
        top_header = [item['dimensionValues'] for item in response['reports'][0]
                      ['columnHeader']['metricHeader']['pivotHeaders'][0]['pivotHeaderEntries']]
        column_metrics = [item['metric']['name'] for item in response['reports'][0]
                          ['columnHeader']['metricHeader']['pivotHeaders'][0]['pivotHeaderEntries']]
        array = np.concatenate((np.array(top_header),
                                np.array(column_metrics).reshape((len(column_metrics),1))), 
                               axis = 1)
        column_index = pd.MultiIndex.from_arrays(np.transpose(array))
        
        # create row index
        try:
            row_index_names = response['reports'][0]['columnHeader']['dimensions']
            row_index = [ element['dimensions'] for element in response['reports'][0]['data']['rows'] ]
            row_index_named = pd.MultiIndex.from_arrays(np.transpose(np.array(row_index)), 
                                                        names = np.array(row_index_names))
        except: 
            row_index_named = None
        # combine into a dataframe
        df = pd.DataFrame(data = np.array(pivot_values), 
                          index = row_index_named, 
                          columns = column_index).astype('float')
    except:
        df = pd.DataFrame()
    return df


def run_report(body, credentials_file):
    #Create service credentials
    credentials = service_account.Credentials.from_service_account_file(credentials_file, 
                                scopes = ['https://www.googleapis.com/auth/analytics.readonly'])
    #Create a service object
    service = build('analyticsreporting', 'v4', credentials=credentials)
    
    #Get GA data
    response = service.reports().batchGet(body=body).execute()
    
    return(response)

def ga_response_dataframe(response):
    """ Función para crear un dataframe de pandas a través de la respuesta de google Analytics
    Args:
        response: respuesta de la petición de Google Analytics
    Returns:
        Dataframe de pandas con las diferentes dimensiones y métricas
    """
    row_list = []

    # Get each collected report
    for report in response.get('reports', []):
        # Set column headers
        column_header = report.get('columnHeader', {})
        dimension_headers = column_header.get('dimensions', [])
        metric_headers = column_header.get('metricHeader', {}).get('metricHeaderEntries', [])

        # Get each row in the report
        for row in report.get('data', {}).get('rows', []):
            # create dict for each row
            row_dict = {}
            dimensions = row.get('dimensions', [])
            date_range_values = row.get('metrics', [])

            # Fill dict with dimension header (key) and dimension value (value)
            for header, dimension in zip(dimension_headers, dimensions):
                row_dict[header] = dimension

            # Fill dict with metric header (key) and metric value (value)
            for i, values in enumerate(date_range_values):
                for metric, value in zip(metric_headers, values.get('values')):
                    # Set int as int, float a float
                    if ',' in value or '.' in value:
                        row_dict[metric.get('name')] = float(value)
                    else:
                        row_dict[metric.get('name')] = int(value)

            row_list.append(row_dict)
    return pd.DataFrame(row_list)

def formato_adecuado(df_adv_report_ads_raw):
    df_adv_report_ads_raw['day'] = pd.to_datetime(df_adv_report_ads_raw['day'])

    return df_adv_report_ads_raw

API_KEYS = 'C:/Users/c_gra/OneDrive/Escritorio/ETL_Datafactory/scripts/api_keys.json'

VIEW_ID = '176313148'
INSIGHTS = {
                'reportRequests': [
                    {
                        'viewId': VIEW_ID,
                        'dateRanges': [{'startDate': '2023-01-31', 'endDate': '2023-02-01'}],
                        'metrics': [{'expression' :'ga:goal1Completions'} , {'expression' :'ga:goal2Completions'}, {'expression' :'ga:goal3Completions'},{'expression' :'ga:goal4Completions'}
                                    ,{'expression' :'ga:goal5Completions'},{'expression' :'ga:goal6Completions'},{'expression' :'ga:goalCompletionsAll'}],
                        'dimensions': [{'name' : 'ga:date'}, {'name':'ga:campaign'}, {'name':'ga:source'}, {'name':'ga:medium'}, 
                                       {'name':'ga:channelGrouping'}, {'name':'ga:dimension4'}, {'name':'ga:hostname'}, 
                                       {'name':'ga:landingPagePath'}, {'name':'ga:fullReferrer'} ],
                        'filtersExpression': '',
                        'pageSize' : 100000,
                        'limit' : 100000
                    }]
            }
report_insights = run_report(INSIGHTS, API_KEYS)
report_insights = ga_response_dataframe(report_insights)
insights_df = round(report_insights , 2)
        
insights_df = insights_df.rename(columns=lambda x: x.split(':')[ 1 ])

insights_df = insights_df.rename(columns=
                    {'date': 'day'}
                    )

insights_df = formato_adecuado(insights_df)

insights_df.to_csv('client_id.csv')
