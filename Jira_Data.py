from jira import JIRA
import re
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import norm
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from datetime import date

def getTickets(status = 'LIVE'):
    '''
    Parameters:
    Status = 
            "LIVE" 
            "Ended\\ -\\ Reporting" 
            "Done" 
            "Concluded" 
    (Mostly Used Status,you can use other JIRA defined status to fetch the data)
    
    This module with help download the JIRA data in Dataframe format 
    It Will download the Following Data Feilds 
    
    Title,
    Description,
    Devices,
    Psychological Principles/Effects,
    Psychological Principles,
    Hypothesis 1 We Observed (Evidence, Feedback),
    Hypothesis 2 We Believe (Change, Groups, Outcomes),
    Hypothesis 3 We will Know When this Happens (Data, Feedback),
    Additional Considerations,
    User Experience - UI,
    experimentType,
    experimentPage,
    desktopliveDate,
    desktopendDate,
    desktoptestTitle,
    desktoptestId,
    desktopcontrolId,
    desktopvariationIds,
    desktopdimension,
    tablet_liveDate,
    tablet_endDate,
    tablet_testTitle,
    tablet_testId,
    tablet_controlId,
    tablet_variationIds,
    tablet_dimension,
    mobileliveDate,
    mobiletestTitle,
    mobiletestId,
    mobilecontrolId,
    mobilevariationIds,
    mobileendDate,
    mobiledimension,
    Keys
    
    '''
    
    # Passwords for JIRA initilization
    options = {'server': "https://endlessgain.atlassian.net"}
    jira = JIRA(options, basic_auth=('pooja.bhat@endlessgain.com', 'WSeX2dWnPw8NyoJqNUOP7BDE'))

    # Feilds to be fetched from JIRA ( You can add more feilds if required)
    Jira_Dict = {'Title':'summary',
                'Description' : 'description',
                'Devices' : 'customfield_10028',

                'Psychological Principles/Effects' : 'customfield_10061',
                'Psychological Principles' : 'customfield_10096',
                 
                'Desktop Variation(s) Description': 'customfield_10097',
                'Tablet Variation(s) Description': 'customfield_10098',
                'Mobile Variation(s) Description': 'customfield_10099',

                'Hypothesis 1 We Observed (Evidence, Feedback)':'customfield_10039',
                'Hypothesis 2 We Believe (Change, Groups, Outcomes)':'customfield_10040',
                'Hypothesis 3 We will Know When this Happens (Data, Feedback)':'customfield_10041',
                'Additional Considerations' : 'customfield_10045',

                'User Experience - UI' : 'customfield_10047',
                'experimentType'      : 'customfield_10094',
                'experimentPage'      :'customfield_10095',

                'desktopliveDate'     : 'customfield_10068',
                'desktopendDate'      : 'customfield_10069',
                'desktoptestTitle'    : 'customfield_10073',
                'desktoptestId'       : 'customfield_10090',
                'desktopcontrolId'    : 'customfield_10074',
                'desktopvariationIds' : 'customfield_10075',
                'desktopdimension'    : 'customfield_10072',

                'tabletliveDate'     : 'customfield_10078',
                'tabletendDate'      : 'customfield_10079',
                'tablettestTitle'    : 'customfield_10081',
                'tablettestId'       : 'customfield_10092',
                'tabletcontrolId'    : 'customfield_10082',
                'tabletvariationIds' : 'customfield_10076',
                'tabletdimension'    : 'customfield_10089',

                'mobileliveDate'      : 'customfield_10083',
                'mobiletestTitle'     : 'customfield_10086',
                'mobiletestId'        : 'customfield_10091',
                'mobilecontrolId'     : 'customfield_10087',
                'mobilevariationIds'  : 'customfield_10077',
                'mobileendDate'       : 'customfield_10084',
                'mobiledimension'     : 'customfield_10085' }

    Jira_Dict_new = {y:x for x,y in Jira_Dict.items()}

    issues = []
    issues_=jira.search_issues(f'status={status}',startAt=0,maxResults=100)
    issues = issues+ issues_
    start = 1
    while len(issues_) == 100:
        issues_=jira.search_issues(f'status={status}',startAt=start,maxResults=100)
        issues = issues+ issues_
        start = start + 100

    issues_list = []
    issue_names = []
    for i in issues:
        issue_names.append(i.key)
        issues_list.append(list(map(i.raw['fields'].get, Jira_Dict_new.keys()) ))
    df = pd.DataFrame(issues_list, columns=Jira_Dict.keys())
    df['Keys'] = issue_names

    def psychClean(x):
        try:
            return ([i['value'] for i in x])
        except:
            return None

    def get_devices(row):
        if row is not None:
            values = [i['value'] for i in row]
            return values
        else:
            return None

    def get_value(x):
        try:
            return x['value']
        except:
            return None

    df['Psychological Principles'] = df['Psychological Principles'].apply(psychClean)

    df['Devices'] = df['Devices'].apply(get_devices)

    df['experimentPage'] = df['experimentPage'].apply(get_value)
    df['experimentType'] = df['experimentType'].apply(get_value)

    df[['JIRA_ID', 'Ticket_Number']] = df['Keys'].str.split('-', expand = True)

    ga_id_mapping = pd.read_excel('GA_ID_Map.xlsx')
    df = pd.merge(df, ga_id_mapping[['Platform', 'Client', 'JIRA_ID', 'GA_View','ForecastModule']])
    
    return df 


def download_attachments(issueId):
    '''
    Parameters : issueId 

    This module will download all the available attachments in the given JIRA ID 

    Note: The issueId is the Jira ticket number NOT THE EXPERIMENT NUMBER 
    '''
    options = {'server': "https://endlessgain.atlassian.net"}
    jira = JIRA(options, basic_auth=('pooja.bhat@endlessgain.com', 'WSeX2dWnPw8NyoJqNUOP7BDE'))

    jira_issue = jira.issue(issueId, expand="attachment")

    attachment_list = []

    for attachment in  jira_issue.fields.attachment  :    
        image = attachment.get()    
        jira_filename = attachment.filename    
        attachment_list.append(jira_filename)
        with open(f'./Template/assets/images/{issueId}/jiraData/'+jira_filename, 'wb') as f:        
            f.write(image) 
    
    print('Images Downloaded')        
    return attachment_list
