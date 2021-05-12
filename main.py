import argparse
from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
import re
import requests
import pandas as pd
import numpy as np
import openpyxl

import os 
from datetime import datetime, timedelta
import pandas as pd 
import dataframe_image as dfi
import plotly.graph_objects as go
import plotly.offline as offline
import time
import pdfkit

from Jira_Data import getTickets, download_attachments

from Significance_Calculation import typeofExperiment
import warnings
warnings.filterwarnings('ignore')

from bellaandDuke import bellaAndDuke
from falvourly import flavourly
from moss import moss
from moda import moda

from Revenue_Forecast_v2 import revenue_forecast, create_graph

from chums import chums
from blindsDirect import blindsDirect
from polesDirect import polesDirect

import smtplib
from email.mime.text import MIMEText
from email.header    import Header
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.utils import formatdate
from email import encoders
from email.mime.application import MIMEApplication

import sys

def missingDataTicket(ticket):
    missingList = []
    singleData, multiDevice, noData,data = typeofExperiment(ticket)
    if noData == True:
        missingList.append('No Data to Process')
        

    if ticket['Hypothesis 1 We Observed (Evidence, Feedback)'] is None:
        missingList.append('Hypothesis 1')
    if ticket['Hypothesis 2 We Believe (Change, Groups, Outcomes)'] is None:
        missingList.append('Hypothesis 2')
    if ticket['Hypothesis 3 We will Know When this Happens (Data, Feedback)'] is None:
        missingList.append('Hypothesis 3')
        
    if data is not None: 
        if data['controlId'] is None:
            missingList.append('Control ID')
        if data['variationId'] is None:
            missingList.append('Variation ID')

    
    if (ticket['Desktop Variation(s) Description'] is None) and ('Desktop' in ticket['Devices']):
        missingList.append('Desktop Variation(s) Description')
    
    if (ticket['Tablet Variation(s) Description'] is None) and ('Tablet' in ticket['Devices']):
        missingList.append('Tablet Variation(s) Description')
    
    if (ticket['Mobile Variation(s) Description'] is None) and ('Mobile' in ticket['Devices']):
        missingList.append('Mobile Variation(s) Description')
    

    if ticket['Psychological Principles'] is None:
        if ticket['Psychological Principles/Effects'] is None:
            missingList.append('Psychological Principles')
    elif ticket['Psychological Principles'][0]   == 'Other':
        if ticket['Psychological Principles/Effects'] is None:
            missingList.append('Psychological Principles')
    
    return missingList


def dataMail(ticket, mail_ids):
    JIRA_ticket = ticket['Title']
    recipients_emails = []
    fields = missingDataTicket(ticket)
    if len(fields) > 0:
        
        missingFields = "\n\n ".join(fields)

        mail_ids = mail_ids.split(';')
        recipients_emails = recipients_emails+ mail_ids

        smtp_host = 'smtp-mail.outlook.com'
        login, password = 'akhil.vsrp@endlessgain.com','Ak@1994hil'

        msg = MIMEText( f'There are missing Fields in Ticket with ID {JIRA_ticket}, Please update these feilds \n\n {missingFields}', 'plain', 'utf-8')
        msg['Subject'] = Header(f'Missing Fields in JIRA ticket {JIRA_ticket}', 'utf-8')
        msg['From'] = login
        msg['To'] = ", ".join(recipients_emails)


        s = smtplib.SMTP(smtp_host, 587, timeout=30)
        # s.set_debuglevel(1)
        try:
            s.starttls()
            s.login(login, password)
            s.sendmail(msg['From'], recipients_emails, msg.as_string())
        finally:
            s.quit()
        
        print("Data Missing Cant Generate Report")
        return False
        
            
    else:
        print("No Missing Fields")
        return True
        
        
def failedMail(ticket, mail_ids, exception):
    JIRA_ticket = ticket['Title']
    recipients_emails = []

    mail_ids = mail_ids.split(';')
    recipients_emails = recipients_emails+ mail_ids

    smtp_host = 'smtp-mail.outlook.com'
    login, password = 'akhil.vsrp@endlessgain.com','Ak@1994hil'

    msg = MIMEText( f'The report generation for {JIRA_ticket} failed. \n\n {exception}', 'plain', 'utf-8')
    msg['Subject'] = Header(f'Report Generation failed {JIRA_ticket}', 'utf-8')
    msg['From'] = login
    msg['To'] = ", ".join(recipients_emails)


    s = smtplib.SMTP(smtp_host, 587, timeout=30)
    # s.set_debuglevel(1)
    try:
        s.starttls()
        s.login(login, password)
        s.sendmail(msg['From'], recipients_emails, msg.as_string())
    finally:
        s.quit()

        

tickets_df_mapped = getTickets(status="Ended\ -\ Reporting")

tickets_df_mapped['desktoptestTitle'] = np.where(tickets_df_mapped['Client'] == 'Blinds Direct',tickets_df_mapped['Title'],tickets_df_mapped['desktoptestTitle'])

for i in range(len(tickets_df_mapped)):
    ticket = tickets_df_mapped.iloc[i]
#     carryon = dataMail(ticket, 'akhil.vsrp@endlessgain.com; steve@endlessgain.com; pooja.bhat@endlessgain.com;accountmanagement@endlessgain.com')
#     time.sleep(10)
    carryon = dataMail(ticket, 'akhil.vsrp@gmail.com')
    if carryon:
        if ticket['Client'] == 'Bella & Duke':
            try:
                bellaAndDuke(ticket)
                print('Here')
            except Exception as e:
                print(ticket['Client'], 'failed')
                failedMail(ticket, 'akhil.vsrp@endlessgain.com', e)

        if ticket['Client'] == 'Flavourly':
            try:
                flavourly(ticket)
            except Exception as e:
                print(ticket['Client'], 'failed')
                failedMail(ticket, 'akhil.vsrp@endlessgain.com', e)


        if ticket['Client'] == 'Moss':
            try:
                moss(ticket)
            except Exception as e:
                print(ticket['Client'], 'failed')
                failedMail(ticket, 'akhil.vsrp@endlessgain.com', e)

        if ticket['Client'] == 'Moda':
            try:
                moda(ticket)
            except Exception as e:
                print(ticket['Client'], 'failed')
                failedMail(ticket, 'akhil.vsrp@endlessgain.com', e)


        if ticket['Client'] == 'Chums':
            try:
                chums(ticket)
            except Exception as e:
                print(ticket['Client'], 'failed')
                failedMail(ticket, 'akhil.vsrp@endlessgain.com', e)

        if ticket['Client'] == 'Blinds Direct':
            if ticket['Title'].startswith('BD'):
                print('Blinds')

                try:
                    blindsDirect(ticket)
                except Exception as e:
                    print(ticket['Client'], 'failed')
                    failedMail(ticket, 'akhil.vsrp@endlessgain.com', e)

            if ticket['Title'].startswith('PD'):
                print('Poles')

                try:
                    polesDirect(ticket)
                except Exception as e:
                    print(ticket['Client'], 'failed')
                    failedMail(ticket, 'akhil.vsrp@endlessgain.com', e)


print("Done")