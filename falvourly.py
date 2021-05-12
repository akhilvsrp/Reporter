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

from Significance_Calculation import data_significance,uplift_calculation,graphingCummulative,dataframeImage
from Significance_Calculation import dataSignificance,typeofExperiment,get_service,finalResult,reportMail
import warnings
warnings.filterwarnings('ignore')

from Revenue_Forecast_v2 import revenue_forecast, create_graph

import smtplib
from email.mime.text import MIMEText
from email.header    import Header
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.utils import formatdate
from email import encoders
from email.mime.application import MIMEApplication

import sys

def flavourly(ticket):

    def get_service(api_name, api_version, scope, client_secrets_path):
        """Get a service that communicates to a Google API.

        Args:
        api_name: string The name of the api to connect to.
        api_version: string The api version to connect to.
        scope: A list of strings representing the auth scopes to authorize for the
        connection.
        client_secrets_path: string A path to a valid client secrets file.

        Returns:
        A service that is connected to the specified API.
        """
        # Parse command-line arguments.
        parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[tools.argparser])
        flags = parser.parse_args([])

        # Set up a Flow object to be used if we need to authenticate.
        flow = client.flow_from_clientsecrets(
        client_secrets_path, scope=scope,
        message=tools.message_if_missing(client_secrets_path))

        # Prepare credentials, and authorize HTTP object with them.
        # If the credentials don't exist or are invalid run through the native client
        # flow. The Storage object will ensure that if successful the good
        # credentials will get written back to a file.
        storage = file.Storage(api_name + '.dat')
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage, flags)
        http = credentials.authorize(http=httplib2.Http())

        # Build the service object.
        service = build(api_name, api_version, http=http)

        return service


    def download_data(startDate,endDate, metrics, dimensions, completeData = False):
        scope = ['https://www.googleapis.com/auth/analytics.readonly']
        service = get_service('analytics', 'v3', scope, 'client_secret.json')
        metrics = ','.join(metrics)
        dimension = ','.join(dimensions)
        
    #     eventAction = 'ga:eventAction=='+eventAction
        

        if completeData:
            data = service.data().ga().get( 
                            ids='ga:65653062',
                            start_date=startDate,
                            end_date=endDate,
                            metrics=metrics,
                            dimensions = dimension).execute()

        else:
            data = service.data().ga().get( 
                                ids='ga:65653062',
                                start_date=startDate,
                                end_date=endDate,
                                metrics=metrics,
                                dimensions = dimension).execute()

        column_names = [i['name'].split(':')[1] for i in data['columnHeaders']]
        
        if 'rows' in data.keys():
            df = pd.DataFrame(data['rows'], columns=column_names)
            print('Data was Fetched')
        else : 
            print('No Data Was Fetched')
            df = pd.DataFrame()
            
        return df 

    def download(data,device,deviceSplit = True):
        dim1 = ['ga:experimentName','ga:experimentCombination']
        dim2 = ['ga:experimentName','ga:deviceCategory', 'ga:experimentCombination']
        dim3 = ['ga:experimentName','ga:userType'      , 'ga:experimentCombination']
        dim4 = ['ga:experimentName','ga:deviceCategory','ga:userType', 'ga:experimentCombination']
        #Line Graphs
        dim5 = ['ga:experimentName','ga:date', 'ga:experimentCombination']
        dim6 = ['ga:experimentName','ga:deviceCategory', 'ga:experimentCombination']
        dim7 = ['ga:yearMonth']

        met=['ga:sessions',
            'ga:users',
            'ga:transactions',
            'ga:transactionRevenue',
            'ga:revenuePerTransaction',
            'ga:transactionsPerSession']

        startDate=data['startDate']
        endDate=data['endDate']
        title=data['title']
        testId=data['testId']
        controlId=data['controlId']
        variationId=data['variationId']
        dimension=data['dimension']
        if endDate is None:
            endDate = 'today'
            
        id_map = {}
        id_map[controlId]='0'
        if variationId is not None:
            for i in range(len(variationId.split(','))):
                id_map[variationId.split(',')[i]] = str(i+1)
        else:
            print('No Variation ID')
            id_map = {'0':'0', '1':'1', '2':'2'}
            
            
        output = {}
        
        df_all_site           = download_data(startDate, endDate, met, [])
        total_users = int(df_all_site['users'][0])
        output["Total_Users"] = total_users  
        
        def dataClean(df):
            df[['experimentID', 'experimentVariant']] = df['experimentCombination'].str.split(':', expand = True) 
            df = df[(df['experimentID'] == testId)]
            df['experimentVariant'] = df['experimentVariant'].map(id_map)
            return df
        
        if deviceSplit:
            df_overall           = download_data(startDate, endDate, met, dim1)
            output['Overall'] = dataClean(df_overall)
            
            #             Devices
            df_device            = download_data(startDate, endDate, met, dim2)
            output['Device'] = dataClean(df_device)
            
            #             UserType
            df_user_type         = download_data(startDate, endDate, met, dim3)
            output['User'] = dataClean(df_user_type)

            #             DailyDevice
            startDay = datetime.strptime(startDate, "%Y-%m-%d")

            if endDate == 'today':
                endDay = str(datetime.now().date())
            else:
                endDay = datetime.strptime(endDate, "%Y-%m-%d")
            noDays =endDay - startDay

            days = []
            for i in range((noDays.days)+1) :
                new_date = startDay + timedelta(days=i)
                days.append(str(new_date.date()))

            print(days)

            day_device = pd.DataFrame()
            for i in range(len(days)):
                day = days[i]
                print(day)
                day_device_int = download_data(startDate, day, met, dim6)
                day_device_int['date'] = day
                day_device_int = dataClean(day_device_int)
                day_device = day_device.append(day_device_int)
                if (i%5 == 0): 
                    time.sleep(1*i)

            df_day_device = day_device
            
            output['DayDevice'] = df_day_device

        else:
            
            df_overall           = download_data(startDate, endDate, met, dim1)
            df_overall['deviceCategory'] =  device
            output['Overall'] = dataClean(df_overall)

            #             UserType
            df_user_type         = download_data(startDate, endDate, met, dim3)
            df_user_type['deviceCategory'] =  device
            output['User'] = dataClean(df_user_type)

            #             DailyDevice
            startDay = datetime.strptime(startDate, "%Y-%m-%d")

            if endDate == 'today':
                endDay = str(datetime.now().date())
            else:
                endDay = datetime.strptime(endDate, "%Y-%m-%d")
            noDays =endDay - startDay

            days = []
            for i in range((noDays.days)+1) :
                new_date = startDay + timedelta(days=i)
                days.append(str(new_date.date()))

            print(days)

            day_device = pd.DataFrame()
            for i in range(len(days)):
                day = days[i]
                print(day)
                day_device_int = download_data(startDate, day, met, dim5)
                day_device_int['deviceCategory'] =  device
                day_device_int['date'] = day
                day_device_int = dataClean(day_device_int)
                day_device = day_device.append(day_device_int)
                if (i%5 == 0): 
                    time.sleep(1*i)

            df_day_device = day_device

            output['Day'] = df_day_device

            
        return output

    def dataDownload(ticket):
        singleData, multiDevice, noData,data = typeofExperiment(ticket)
        
        if noData == True :
            print('Send Email') 

        else:
            if singleData:
                if multiDevice:
                    print(data)
                    output = download(data,'',deviceSplit = True)
                else:
                    output = {}
                    print(data)
                    output[ticket['Devices'][0].lower()] = download(data,ticket['Devices'][0].lower(),deviceSplit = False)
            else:
                
                if 'Desktop' in ticket['Devices']:
                    startDate = ticket[f"desktopliveDate"]
                    endDate = ticket[f"desktopendDate"]
                    title = ticket[f"desktoptestTitle"]
                    testId = ticket[f"desktoptestId"]
                    controlId = ticket[f"desktopcontrolId"]
                    variationId = ticket[f"desktopvariationIds"]
                    dimension = ticket[f"desktopdimension"]

                    data = {'startDate':startDate,
                            'endDate':endDate,
                            'title':title,
                            'testId':testId,
                            'controlId':controlId,
                            'variationId':variationId,
                            'dimension':dimension}
                    print(data)
                    output_desktop = download(data,'desktop',deviceSplit = False)
                    output['desktop'] = output_desktop

                if 'Mobile' in ticket['Devices']:
                    startDate = ticket["mobileliveDate"]
                    endDate = ticket["mobileendDate"]
                    title = ticket["mobiletestTitle"]
                    testId = ticket["mobiletestId"]
                    controlId = ticket["mobilecontrolId"]
                    variationId = ticket["mobilevariationIds"]
                    dimension = ticket["mobiledimension"]

                    data = {'startDate':startDate,
                            'endDate':endDate,
                            'title':title,
                            'testId':testId,
                            'controlId':controlId,
                            'variationId':variationId,
                            'dimension':dimension}

                    print(data)
                    output_mobile =download(data,'mobile',deviceSplit = False)
                    output['mobile'] = output_mobile


                if 'Tablet' in ticket['Devices']:
                    startDate = ticket["tabletliveDate"]
                    endDate = ticket["tabletendDate"]
                    title = ticket["tablettestTitle"]
                    testId = ticket["tablettestId"]
                    controlId = ticket["tabletcontrolId"]
                    variationId = ticket["tabletvariationIds"]
                    dimension = ticket["tabletdimension"]

                    data = {'startDate':startDate,
                            'endDate':endDate,
                            'title':title,
                            'testId':testId,
                            'controlId':controlId,
                            'variationId':variationId,
                            'dimension':dimension}

                    print(data)
                    output_tablet = download(data,'tablet',deviceSplit = False)
                    output['tablet'] = output_tablet
                    
            return output

    daat = dataDownload(ticket)

    location = ticket['Keys']

    try:
        os.mkdir(f'./Template/assets/images/{location}')
    except:
        print('Directory Present')
        
    try:
        os.mkdir(f'./Template/assets/images/{location}/jiraData')
    except:
        print('Directory Present')
    try:
        os.mkdir(f'./Template/assets/images/{location}/tables')
    except:
        print('Directory Present')    
    try:    
        os.mkdir(f'./Template/assets/images/{location}/graphs')
    except:
        print('Directory Present')
        
    attachments = download_attachments(ticket['Keys'])

    data,graphData,combined_data = dataSignificance(daat)

    try:
        graphingCummulative(graphData, ticket['Keys'])
    except:
        print('More than 1 Variant')
    try:
        graphingCummulative2(graphData, ticket['Keys'])
    except:
        print('More than 2 Variant')

    header_list = ['User_Returning Visitor']
    if "Overall" in combined_data.keys():
        header_list.append("Overall")
    elif "Device_desktop" in combined_data.keys():
        header_list.append("Device_desktop")
    elif "Device_mobile" in combined_data.keys():
        header_list.append("Device_mobile")
    elif "Device_tablet" in combined_data.keys():
        header_list.append("Device_tablet")
        
    for i in combined_data.keys():
        if i in header_list:
            dfi.export(dataframeImage(combined_data[i],header = True),f'./Template/assets/images/{location}/tables/{i}.jpg')
        else:
            dfi.export(dataframeImage(combined_data[i]),f'./Template/assets/images/{location}/tables/{i}.jpg')



    if 'Desktop' in ticket['Devices']:
        startDate = ticket['desktopliveDate']
        endDate = ticket['desktopendDate']
        eventAction = ticket['desktoptestId']
    else:
        startDate = ticket['mobileliveDate']
        endDate = ticket['mobileendDate']
        eventAction = ticket['mobiletestId']
        if eventAction is None:
            startDate = ticket['tablet_liveDate']
            endDate = ticket['tablet_endDate']
            eventAction = ticket['tablet_testId']

    if endDate is None:
        endDate = str(datetime.now().date())

    startDay = datetime.strptime(startDate, "%Y-%m-%d")

    if endDate is None:
        endDay = datetime.strptime(str(datetime.now().date()), "%Y-%m-%d")
    else:
        endDay = datetime.strptime(endDate, "%Y-%m-%d")
    noDays =endDay - startDay


    if ticket['ForecastModule']:    
        met = ['ga:sessions','ga:users','ga:transactions',
            'ga:transactionRevenue','ga:revenuePerTransaction','ga:transactionsPerSession']
        df_forecast = download_data('2010-01-01', 'today', met, ['ga:yearMonth'])
        df_forecast['yearMonth'] = df_forecast['yearMonth'].apply(lambda x:datetime.strptime(x, '%Y%m'))
        df_forecast['transactionRevenue'] = df_forecast['transactionRevenue'].astype('float')
        df_forecast.index = df_forecast['yearMonth']
        df_forecast = df_forecast[df_forecast['transactionRevenue']!=0]

    revenue = []
    for i in data.keys():
        val = uplift_calculation(data[i],daat['Total_Users'],startDate, endDate )
        
        if i == 'Overall':
            Overall_chance = val['Chance Of Being Best']
            Overall_UCR_Change = data[i]['UCR_Change'].sum()*100
            Overall_Uplift = val['Uplift']
        if i == 'Device_desktop':
            desktop_chance = val['Chance Of Being Best']
            desktop_UCR_Change = data[i]['UCR_Change'].sum()*100
            desktop_Uplift = val['Uplift']
        if i == 'Device_mobile':
            mobile_chance = val['Chance Of Being Best']
            mobile_UCR_Change = data[i]['UCR_Change'].sum()*100
            mobile_Uplift = val['Uplift']
        if i == 'Device_tablet':
            tablet_chance = val['Chance Of Being Best']
            tablet_UCR_Change = data[i]['UCR_Change'].sum()*100
            tablet_Uplift = val['Uplift']
            
        if ticket['ForecastModule']:    
            try:
                result,annual_revenue_forecast,annual_revenue_uplift,revenue_diff=revenue_forecast(df_forecast[['transactionRevenue']],val['Uplift']/100)
                print(annual_revenue_forecast,annual_revenue_uplift,revenue_diff)
                revenue.append([i,val['Chance Of Being Best'], val['Change in Revenue'], val['Uplift'], revenue_diff, data[i]['UCR_Change'].sum()*100] )
            except:
                print(i,"failed")
                revenue.append([i,val['Chance Of Being Best'], val['Change in Revenue'], val['Uplift'], val['Change in Revenue'], data[i]['UCR_Change'].sum()*100])
        else:
            revenue.append([i,val['Chance Of Being Best'], val['Change in Revenue'], val['Uplift'], val['Change in Revenue'], data[i]['UCR_Change'].sum()*100])


    df_new = pd.DataFrame(revenue, columns=['', 'Chance of Being Best', 'Revenue Calculated', 'Uplift', 'Revenue Uplift','UCR Change'])
    df_new = df_new.sort_values(['Chance of Being Best'],ascending=[False])
    df_new['Revenue Uplift']= df_new['Revenue Uplift'].apply(lambda x:'\xA3'+'{:20,}'.format(x))
    df_new['Revenue Uplift'] = np.where(df_new['Chance of Being Best'] < 90, '-', df_new['Revenue Uplift'])

    df_new[''] = df_new[''].replace('Device_','', regex=True)
    df_new[''] = df_new[''].replace('mobile','Mobile', regex=True)
    df_new[''] = df_new[''].replace('tablet','Tablet', regex=True)
    df_new[''] = df_new[''].replace('desktop','Desktop', regex=True)
    df_new[''] = df_new[''].replace('_',' ', regex=True)
    df_new = df_new.sort_values(by = ['Chance of Being Best','Revenue Uplift'],ascending=[False,False])

    df_new['Chance of Being Best'] = df_new['Chance of Being Best'].round(2).apply(lambda x:str(x)+'%')
    df_new['Uplift'] = df_new['Uplift'].round(2).apply(lambda x:str(x)+'%')
    df_new['UCR Change'] = df_new['UCR Change'].round(2).apply(lambda x:str(x)+'%')

    df_res = df_new.copy()
    df_styled= df_new[['','UCR Change', 'Chance of Being Best', 'Revenue Uplift']].astype(str).reset_index(drop=True).style.set_table_styles(
        [{'selector': 'th',                           
            'props': [('background-color', 'white')]
        }]).set_properties(**{'background-color': "white"}).hide_index()
    dfi.export(df_styled,f'./Template/assets/images/{location}/tables/All_Combinations.jpg')  

    df_new['inter'] = df_new[''].apply(lambda x: len(x.split(' ')))
    df_res = df_new.copy()

    forecastGraph = False
    if ticket['ForecastModule']:    
        try:
            result,annual_revenue_forecast,annual_revenue_uplift,revenue_diff=revenue_forecast(df_forecast[['transactionRevenue']],Overall_Uplift/100)
            revenue_graph = create_graph(df_forecast[['transactionRevenue']],result, location)
            forecastGraph = True
        except Exception as e:
            df_values = df_new[df_new[''] == 'Overall'].reset_index(drop = True)
            annual_revenue_forecast = df_values['Revenue Calculated'][0]*12
            revenue_diff = df_values['Revenue Calculated'][0]*12
            forecastGraph = False
    else:
        df_values = df_new[df_new['inter'] == 2].reset_index(drop = True)
        if len(df_values) >0:
            annual_revenue_forecast = df_values['Revenue Calculated'][0]*12
            revenue_diff = df_values['Revenue Calculated'][0]*12
        else:
            annual_revenue_forecast = 0
            revenue_diff = 0
            


    psych_tech = pd.read_csv('PsychologicalTechniques.csv')

    multiple_variants = False
    df_var1 = df_new[(df_new[''].str.contains('var1')) & (df_new['inter'] == 2)]
    if df_new[''].str.contains('var2').sum() > 0:
        f = open("./Template/double_variation.html", "r")
        html_text = f.read()
        multiple_variants = True
        df_var2 = df_new[(df_new[''].str.contains('var2')) & (df_new['inter'] == 2)]
    else:
        f = open("./Template/single_variation.html", "r")
        html_text = f.read()



    def createHtmlList(itemList):
        return ' '.join([f'<li>{list_item}</li>' for list_item in itemList])

    if 'Desktop' in ticket['Devices']:
        html_text = html_text.replace('{desktopExists}','' )
        html_text = html_text.replace('{desktopExistsEnd}','' )
    else:
        html_text = html_text.replace('{desktopExists}','<!--')
        html_text = html_text.replace('{desktopExistsEnd}','-->')
        
    if 'Mobile' in ticket['Devices']:
        html_text = html_text.replace('{mobileExists}','' )
        html_text = html_text.replace('{mobileExistsEnd}','' )
    else:
        html_text = html_text.replace('{mobileExists}','<!--')
        html_text = html_text.replace('{mobileExistsEnd}','-->')
        
    if 'Tablet' in ticket['Devices']:
        html_text = html_text.replace('{tabletExists}','' )
        html_text = html_text.replace('{tabletExistsEnd}','' )
    else:
        html_text = html_text.replace('{tabletExists}','<!--')
        html_text = html_text.replace('{tabletExistsEnd}','-->')


    if forecastGraph:
        html_text = html_text.replace('{revenueForecastGraph}','' )
        html_text = html_text.replace('{revenueForecastGraphEnd}','' )
    else:
        html_text = html_text.replace('{revenueForecastGraph}','<!--')
        html_text = html_text.replace('{revenueForecastGraphEnd}','-->')    

    html_text = html_text.replace('{location}',location )

    html_text = html_text.replace('{Client}', ticket['Client'])
    html_text = html_text.replace('{Test_Name}', ticket['Title'])

    try:
        html_text = html_text.replace('{Hypothesis_1}', ticket['Hypothesis 1 We Observed (Evidence, Feedback)'])
    except:
        html_text = html_text.replace('{Hypothesis_1}', '')
        

    try:
        html_text = html_text.replace('{Hypothesis_2}', ticket['Hypothesis 2 We Believe (Change, Groups, Outcomes)'])
    except:
        html_text = html_text.replace('{Hypothesis_2}', '')

    try:
        html_text = html_text.replace('{Hypothesis_3}', ticket['Hypothesis 3 We will Know When this Happens (Data, Feedback)'])
    except:
        html_text = html_text.replace('{Hypothesis_3}', '')



    try:
        if ticket['Psychological Principles'][0] is None:
            html_text = html_text.replace('{Pysch_Principles}', ticket['Psychological Principles/Effects'].split(',')[0])
        elif ticket['Psychological Principles'][0]   == 'Other':
            html_text = html_text.replace('{Pysch_Principles}', ticket['Psychological Principles/Effects'].split(',')[0])
        else:  
            html_text = html_text.replace('{Pysch_Principles}', ticket['Psychological Principles'][0])
    except:
        html_text = html_text.replace('{Pysch_Principles}', ticket['Psychological Principles/Effects'].split(',')[0])
    finally:
        html_text = html_text.replace('{Pysch_Principles}', '')
        
        
    try:    
        df_psych = psych_tech[psych_tech['Technique'] ==  ticket['Psychological Principles'][0]]
        if len(df_psych) > 0:
            html_text = html_text.replace('{Pysch_Principle_Description}', df_psych.iloc[0]['Description'])
        else:
            html_text = html_text.replace('{Pysch_Principle_Description}', '')
    except:
        html_text = html_text.replace('{Pysch_Principle_Description}', '')
        
        
    try:
        html_text = html_text.replace('{desktop_change_description}', ticket['Desktop Variation(s) Description'])
    except:
        html_text = html_text.replace('{desktop_change_description}', '')
        
    try:    
        html_text = html_text.replace('{mobile_change_description}', ticket['Mobile Variation(s) Description'])
    except:
        html_text = html_text.replace('{mobile_change_description}', '')
        
    try:
        html_text = html_text.replace('{tablet_change_description}', ticket['Tablet Variation(s) Description'])
    except:
        html_text = html_text.replace('{tablet_change_description}', '')

    html_text = html_text.replace('{desktop_change_description_2}', '')
    html_text = html_text.replace('{mobile_change_description_2}', '')
    html_text = html_text.replace('{tablet_change_description_2}', '')



    html_text = html_text.replace('{StartDate}', datetime.strftime(datetime.strptime(startDate, "%Y-%m-%d"), "%d-%m-%Y").replace('-','.'))
    html_text = html_text.replace('{EndDate}', datetime.strftime(datetime.strptime(endDate, "%Y-%m-%d"), "%d-%m-%Y").replace('-','.'))
    html_text = html_text.replace('{noOfDays}', str(noDays.days))

    html_text = html_text.replace('{Device_List}',', '.join(ticket['Devices']))
    html_text = html_text.replace('{RevenueUplift}',df_new.iloc[0]['Revenue Uplift'].replace(' ',''))


    try:
        df_var1_d = df_var1[df_var1[''].str.contains('Desktop')]
        html_text = html_text.replace('{DesktopLift}',(df_var1_d.iloc[0]['UCR Change']).replace(' ',''))
        html_text = html_text.replace('{DesktopChance}',(df_var1_d.iloc[0]['Chance of Being Best']).replace(' ',''))
    except :
        html_text = html_text.replace('{DesktopLift}','')
        html_text = html_text.replace('{DesktopChance}','')

    try:
        df_var1_t = df_var1[df_var1[''].str.contains('Tablet')]
        html_text = html_text.replace('{TabletLift}',(df_var1_t.iloc[0]['UCR Change']).replace(' ',''))
        html_text = html_text.replace('{TabletChance}',(df_var1_t.iloc[0]['Chance of Being Best']).replace(' ',''))
    except:
        html_text = html_text.replace('{TabletLift}','')
        html_text = html_text.replace('{TabletChance}','')

    try:
        df_var1_m = df_var1[df_var1[''].str.contains('Mobile')]
        html_text = html_text.replace('{MobileLift}',(df_var1_m.iloc[0]['UCR Change']).replace(' ',''))
        html_text = html_text.replace('{MobileChance}',(df_var1_m.iloc[0]['Chance of Being Best']).replace(' ',''))
        
    except:
        html_text = html_text.replace('{MobileLift}','')
        html_text = html_text.replace('{MobileChance}','')


    if multiple_variants:
        try:
            df_var2_d = df_var2[df_var2[''].str.contains('Desktop')]
            html_text = html_text.replace('{DesktopLift2}',(df_var2_d.iloc[0]['UCR Change']).replace(' ',''))
            html_text = html_text.replace('{DesktopChance2}',(df_var2_d.iloc[0]['Chance of Being Best']).replace(' ',''))
        except:
            html_text = html_text.replace('{DesktopLift2}','')
            html_text = html_text.replace('{DesktopChance2}','')

        try:
            df_var2_t = df_var2[df_var2[''].str.contains('Tablet')]
            html_text = html_text.replace('{TabletLift2}',(df_var2_t.iloc[0]['UCR Change']).replace(' ',''))
            html_text = html_text.replace('{TabletChance2}',(df_var2_t.iloc[0]['Chance of Being Best']).replace(' ',''))
        except:
            html_text = html_text.replace('{TabletLift2}','')
            html_text = html_text.replace('{TabletChance2}','')

        try:
            df_var2_m = df_var2[df_var2[''].str.contains('Mobile')]
            html_text = html_text.replace('{MobileLift2}',(df_var2_m.iloc[0]['UCR Change']).replace(' ',''))
            html_text = html_text.replace('{MobileChance2}',(df_var2_m.iloc[0]['Chance of Being Best']).replace(' ',''))
            
        except:
            html_text = html_text.replace('{MobileLift2}','')
            html_text = html_text.replace('{MobileChance2}','')



    try:
        html_text = html_text.replace('{NoOfUsers}','{:20,}'.format(data['Overall']['users'].sum()).replace(' ',''))
        html_text = html_text.replace('{NoOfTransactions}','{:20,}'.format(data['Overall']['transactions'].sum()).replace(' ',''))
    except:
        df_all_dev = pd.concat([data[i] for i in data.keys() if i.startswith('var1_Device')])
        html_text = html_text.replace('{NoOfUsers}','{:20,}'.format(df_all_dev['users'].sum()).replace(' ',''))
        html_text = html_text.replace('{NoOfTransactions}','{:20,}'.format(df_all_dev['transactions'].sum()).replace(' ',''))

        
    df_new_overall = df_new[(df_new[''].str.contains('Overall')) & (df_new['inter'] == 2)]

    if len(df_new_overall) >0 :
        html_text = html_text.replace('{overallExists}','' )
        html_text = html_text.replace('{overallExistsEnd}','' )
        html_text = html_text.replace('{overallExistsNeg}','<!----')
        html_text = html_text.replace('{overallExistsEndNeg}','---->')
        
        html_text = html_text.replace('{returnoverallExists}','' )
        html_text = html_text.replace('{returnoverallExistsEnd}','' )

        html_text = html_text.replace('{returnoverallExistsNeg}','<!----')
        html_text = html_text.replace('{returnoverallExistsEndNeg}','---->')

        if 'Desktop' in ticket['Devices']:
            html_text = html_text.replace('{desktopExists2}','' )
            html_text = html_text.replace('{desktopExistsEnd2}','' )
        else:
            html_text = html_text.replace('{desktopExists2}','<!- -')
            html_text = html_text.replace('{desktopExistsEnd2}','- ->')

        if 'Mobile' in ticket['Devices']:
            html_text = html_text.replace('{mobileExists2}','' )
            html_text = html_text.replace('{mobileExistsEnd2}','' )
        else:
            html_text = html_text.replace('{mobileExists2}','<!- -')
            html_text = html_text.replace('{mobileExistsEnd2}','- ->')

        if 'Tablet' in ticket['Devices']:
            html_text = html_text.replace('{tabletExists2}','' )
            html_text = html_text.replace('{tabletExistsEnd2}','' )
        else:
            html_text = html_text.replace('{tabletExists2}','<!- -')
            html_text = html_text.replace('{tabletExistsEnd2}','- ->')

    else:
        if len(ticket['Devices']) > 1:
            html_text = html_text.replace('{returnoverallExists}','<!----')
            html_text = html_text.replace('{returnoverallExistsEnd}','---->')
            html_text = html_text.replace('{returnoverallExistsNeg}','')
            html_text = html_text.replace('{returnoverallExistsEndNeg}','')
        else:
            html_text = html_text.replace('{returnoverallExists}','' )
            html_text = html_text.replace('{returnoverallExistsEnd}','' )
            
            html_text = html_text.replace('{returnoverallExistsNeg}','<!----')
            html_text = html_text.replace('{returnoverallExistsEndNeg}','---->')
            
            html_text = html_text.replace('{desktopExists2}','<!- -')
            html_text = html_text.replace('{desktopExistsEnd2}','- ->')
            html_text = html_text.replace('{mobileExists2}','<!- -')
            html_text = html_text.replace('{mobileExistsEnd2}','- ->')
            html_text = html_text.replace('{tabletExists2}','<!- -')
            html_text = html_text.replace('{tabletExistsEnd2}','- ->')
        
        html_text = html_text.replace('{overallExists}','<!----')
        html_text = html_text.replace('{overallExistsEnd}','---->')
        html_text = html_text.replace('{overallExistsNeg}','' )
        html_text = html_text.replace('{overallExistsEndNeg}','' )
        
        if 'Desktop' in ticket['Devices']:
            html_text = html_text.replace('{desktopExists2}','' )
            html_text = html_text.replace('{desktopExistsEnd2}','' )
        else:
            html_text = html_text.replace('{desktopExists2}','<!--')
            html_text = html_text.replace('{desktopExistsEnd2}','-->')

        if 'Mobile' in ticket['Devices']:
            html_text = html_text.replace('{mobileExists2}','' )
            html_text = html_text.replace('{mobileExistsEnd2}','' )
        else:
            html_text = html_text.replace('{mobileExists2}','<!--')
            html_text = html_text.replace('{mobileExistsEnd2}','-->')

        if 'Tablet' in ticket['Devices']:
            html_text = html_text.replace('{tabletExists2}','' )
            html_text = html_text.replace('{tabletExistsEnd2}','' )
        else:
            html_text = html_text.replace('{tabletExists2}','<!--')
            html_text = html_text.replace('{tabletExistsEnd2}','-->')
        
    try:
    
        if len(df_new_overall) >0 :
            html_text = html_text.replace('{conversionRate}',df_new_overall.iloc[0]['UCR Change'])
            html_text = html_text.replace('{chanceOfBeatingControl}',df_new_overall.iloc[0]['Chance of Being Best'])

            if (df_new_overall.iloc[0]['Revenue Uplift'] != '-'):
                html_text = html_text.replace('{RevenueFigure}','<b>'+df_new_overall.iloc[0]['Revenue Uplift'].replace(' ','') + "</b>forecasted revenue uplift over 12 months.")
            else:
                html_text = html_text.replace('{RevenueFigure}','')
        else:
            df_new_overall = df_new[(df_new['inter'] == 2)]
            html_text = html_text.replace('{conversionRate}',df_new_overall.iloc[0]['UCR Change'])
            html_text = html_text.replace('{chanceOfBeatingControl}',df_new_overall.iloc[0]['Chance of Being Best'])

            if (df_new_overall.iloc[0]['Revenue Uplift'] != '-'):
                html_text = html_text.replace('{RevenueFigure}','<b>'+df_new_overall.iloc[0]['Revenue Uplift'].replace(' ','') + "</b>forecasted revenue uplift over 12 months.")
            else:
                html_text = html_text.replace('{RevenueFigure}','')
    except Exception as e:
        print(e)
        
        
        
            
    html_text = html_text.replace('{todayDate}',str(datetime.now().date()))
    html_text = html_text.replace('{experimentResultText}',finalResult(df_res))

    file_name = f"./Template/{ticket['Title'].replace('/','')}.html"
    Html_file= open(file_name,"w")
    Html_file.write(html_text)
    Html_file.close()


    options = {
        'page-size': 'Legal',
        'orientation' : 'landscape',
        'margin-top': '0in',
        'margin-right': '0in',
        'margin-bottom': '0in',
        'margin-left': '0in',
        'no-outline': None,
        'dpi': 1000
        }
    pdfkit.from_file(file_name, f"./PDF/{ticket['Title'].replace('/','')}.pdf", options=options)

#     reportMail(ticket, 'akhil.vsrp@endlessgain.com;pooja.bhat@endlessgain.com')

