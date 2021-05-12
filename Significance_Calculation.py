import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import norm
from datetime import datetime
from itertools import combinations
import dataframe_image as dfi
import plotly.graph_objects as go
import plotly.offline as offline
from calendar import monthrange
import argparse

import smtplib
from email.mime.text import MIMEText
from email.header    import Header
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.utils import formatdate
from email import encoders
from email.mime.application import MIMEApplication

import sys

def UCR_ECR(df):
    '''
    Calculate User Conversion Rare and E-Commerce Conversion Rate With change in User Conversion Rate and E-Commerce Conversion Rate
    '''
    
    df =  df.apply(pd.to_numeric, errors='coerce')

    df['UCR'] = df['transactions'].astype(int)/df['users'].astype(int)
    df['ECR'] = df['transactions'].astype(int) /df['sessions'].astype(int)

    control_data = df[df['experimentVariant'] == 0][['experimentName', 'UCR', 'ECR']].rename({'UCR' : 'UCR_0',
                                                                                              'ECR' : 'ECR_0'}, axis=1)
    df = df.merge(control_data, on = ('experimentName'), how = 'left')

    df['UCR_Change'] = (df['UCR'] - df['UCR_0']) / df['UCR_0']
    df['ECR_Change'] = (df['ECR'] - df['ECR_0']) / df['ECR_0']

    return df

def significance(control_users, control_transactions, variant_users, variant_transactions):

    #Calculating Conversion Rates

    control_conversion_rate = control_transactions/control_users
    variant_conversion_rate = variant_transactions/variant_users

    #Calculating Standard Error

    control_standard_error = np.sqrt((control_conversion_rate *(1-control_conversion_rate))/control_users )
    variant_standard_error = np.sqrt((variant_conversion_rate *(1-variant_conversion_rate))/variant_users )

    # Z Score

    a = (control_conversion_rate - variant_conversion_rate)
    b = np.sqrt(control_standard_error**2 + variant_standard_error**2)

    Z_score = a/b

    # Calculating Normal Distribution Cummulative
    P_Value = norm.cdf(Z_score)

    return {'control_conversion_rate' : round(control_conversion_rate * 100, 2),
            'variant_conversion_rate' : round(variant_conversion_rate * 100, 2),
            'control_standard_error' : round(control_standard_error * 100, 2),
            'variant_standard_error' : round(variant_standard_error * 100, 2),
            'Z_score' : round(Z_score * 100, 2),
            'P_Value' : round(P_Value, 2),
           'Statistical_Significance' : round((1-P_Value) *100, 1)}

def significance_df(df):
    
    '''
    Parameters: 
    DataFrame : This DataFrame should Contain 
    experimentVariant, 
    experimentName,
    Users,
    Sessions
    Transactions,
    revenuePerTransaction,
    transactionRevenue,def dataframeImage(df, header = False):
    df['Revenue Per User'] = df['transactionRevenue']/ df['users']
    
    '''
    
    # Calculate User Conversion Rate and Ecommerce Conversion Rate
    df = UCR_ECR(df)
    
    # Calculate the Significance of the Experiment
    df_sig = df[['experimentVariant', 'users' , 'transactions']]

    for i in df_sig.to_dict(orient = 'record'):
        if i['experimentVariant'] == 0:
            control_users = int(i['users'])
            control_transactions = int(i['transactions'])
        else:
            variant_users = int(i['users'])
            variant_transactions = int(i['transactions'])

    value = significance(control_users,control_transactions,variant_users, variant_transactions )

    df['Chance_Of_Being_Best'] = value['Statistical_Significance']

    df['UCR'] = df['UCR'] * 100 
    df['ECR'] = df['ECR'] * 100

    df_out = df[['experimentVariant','sessions', 'users',
             'transactions', 'transactionRevenue', 'revenuePerTransaction',
            'transactionsPerSession','UCR','UCR_Change', 'ECR','ECR_Change','Chance_Of_Being_Best']]

    df_out['Chance_Of_Being_Best'][df_out['experimentVariant'] == 0] = 0
    
    df_out = df_out.rename({'UCR' : 'User Conversion Rate','ECR' : 'E-Commerce Conversion Rate' }, axis =1 )

    return df_out

def control_variant(df):
    df['users'] = df['users'].astype(int)
    df['transactions'] = df['transactions'].astype(int)
    df['experimentVariant'] = df['experimentVariant'].astype(str)

    df = pd.pivot_table(df,values=['users','transactions'], index='date', columns = 'experimentVariant').reset_index()
    df.columns = [''.join(i) for i in df.columns]
    df = df.apply(pd.to_numeric)


    df['Control'] = (df['transactions0']/df['users0'])*100
    df['variant'] = (df['transactions1']/df['users1'])*100
    
    df['Variant_'] = df['variant'] - df['Control']
    df['Control_'] = df['Control'] - df['Control']
    
    return df

def dataSignificance(data):
    export_data = {}
    export_graphs = {}
    device = False
    combined_data = {}
       
    def deviceCalc(df):
        df_splices = []
        for i in range(1, len(df['experimentVariant'].unique())):
            df_splices.append(df[df['experimentVariant'].isin({'0', str(i)})])
            for j in df_splices[i-1]['deviceCategory'].unique():
                df_spliced = df_splices[i-1][df_splices[i-1]['deviceCategory'] == j]
                if len(df_spliced) == 2:
                    export_data[f'var{i}_Device_{j}'] = significance_df(df_spliced)
                else:
                    print(f"No {j} Device")

            for comb in combinations(df['deviceCategory'].unique(), 2):
                df_int = df[df['deviceCategory'].isin(comb)]
                df_int['sessions'] = df_int['sessions'].astype(int)
                df_int['users'] = df_int['users'].astype(int)
                df_int['transactions'] = df_int['transactions'].astype(int)
                df_int['transactionRevenue'] = df_int['transactionRevenue'].astype(float)
                df_mix = df_int.groupby('experimentVariant')['sessions', 'users', 'transactions','transactionRevenue'].sum().reset_index()
                df_mix['experimentName'] = df_int['experimentName'].unique()[0]
                df_mix['revenuePerTransaction'] = df_mix['transactionRevenue']/df_mix['transactions']
                df_mix['transactionsPerSession'] = (df_mix['transactions']/df_mix['sessions'])*100
                export_data[f"var{i}_Device_{'_'.join(comb)}"] = significance_df(df_mix)

    def userCalc(df):
        df_splices = []
        for i in range(1, len(df['experimentVariant'].unique())):
            df_splices.append(df[df['experimentVariant'].isin({'0', str(i)})])
            for j in df_splices[i-1]['userType'].unique():
                df_spliced = df_splices[i-1][df_splices[i-1]['userType'] == j]
                if len(df_spliced) == 2:
                    export_data[f'var{i}_User_{j}'] = significance_df(df_spliced)
                else:
                    print(f"No {j} User")
    
    def deviceUserCalc(df):
        df_splices = []
        for i in range(1, len(df['experimentVariant'].unique())):
            df_splices.append(df[df['experimentVariant'].isin({'0', str(i)})])
            df_dev_usr = df_splices[i-1]
            for u in df_dev_usr['userType'].unique():
                for d in df_dev_usr['deviceCategory'].unique():
                    df_spliced = df_dev_usr[(df_dev_usr['userType'] == u) & (df_dev_usr['deviceCategory'] == d)]
                    if len(df_spliced) == 2:
                        export_data[f'var{i}_{d}_{u}'] = significance_df(df_spliced)
                    else:
                        print("Not enough Data")

    
    def dayCalc(df):
        if len(df) > 1:
            df['sessions'] = df['sessions'].astype(int)
            df['transactions'] = df['transactions'].astype(int)
            df['experimentVariant'] = df['experimentVariant'].astype(str)
            df['date'] = df['date'].replace('-','', regex=True).astype(int)
            if len(df['experimentVariant'].unique()) ==2 :
                export_graphs['Daily_Cummulative'] = control_variant(df)

            if len(df['experimentVariant'].unique()) > 2 :
                df_splice1 = df[df['experimentVariant'].isin({'0','1'})]
                export_graphs['var1_Daily_Cummulative'] = control_variant(df_splice1)
                
                df_splice2 = df[df['experimentVariant'].isin({'0','2'})]
                df_splice2['experimentVariant'] = df_splice2['experimentVariant'].replace('2','1')
                export_graphs['var2_Daily_Cummulative'] = control_variant(df_splice2)
    
    def dayDeviceCalc(df):
        if len(df) > 1:
            df['sessions'] = df['sessions'].astype(int)
            df['transactions'] = df['transactions'].astype(int)
            df['experimentVariant'] = df['experimentVariant'].astype(str)
            df['date'] = df['date'].replace('-','', regex=True).astype(int)
            if len(df['experimentVariant'].unique()) == 2 :
                for i in df['deviceCategory'].unique():
                    df_spliced = df[df['deviceCategory'] == i]
                    export_graphs[f'Daily_Cummulative_{i}'] = control_variant(df_spliced)
            
            if len(df['experimentVariant'].unique()) > 2 :
                df_splice1 = df[df['experimentVariant'].isin({'0','1'})]
                for i in df_splice1['deviceCategory'].unique():
                    df_spliced = df_splice1[df_splice1['deviceCategory'] == i]
                    export_graphs[f'Var1_Daily_Cummulative_{i}'] = control_variant(df_spliced)
                    
                df_splice2 = df[df['experimentVariant'].isin({'0','2'})]
                df_splice2['experimentVariant'] = df_splice2['experimentVariant'].replace('2','1')
                for i in df_splice2['deviceCategory'].unique():
                    df_spliced = df_splice2[df_splice2['deviceCategory'] == i]
                    export_graphs[f'Var2_Daily_Cummulative_{i}'] = control_variant(df_spliced)


    if 'desktop' in data.keys():
        device = True
        data_ = data['desktop']
        print('Desktop')
        if 'Device' in data_.keys():
            df = data_['Device']
            deviceCalc(df)

        if 'User' in data_.keys():
            df = data_['User']
            deviceUserCalc(df)

        if 'DayDevice' in data_.keys():
            df = data_['DayDevice']
            dayDeviceCalc(df)

    if 'mobile' in data.keys():
        device = True
        print('Mobile')
        data_ = data['mobile']
        if 'Device' in data_.keys():
            df = data_['Device']
            deviceCalc(df)

        if 'User' in data_.keys():
            df = data_['User']
            deviceUserCalc(df)

        if 'DayDevice' in data_.keys():
            df = data_['DayDevice']
            dayDeviceCalc(df)
            
            
    if 'tablet' in data.keys():
        device = True
        print('Tablet')
        data_ = data['tablet']
        if 'Device' in data_.keys():
            df = data_['Device']
            deviceCalc(df)
        if 'User' in data_.keys():
            df = data_['User']
            deviceUserCalc(df)

        if 'DayDevice' in data_.keys():
            df = data_['DayDevice']
            dayDeviceCalc(df)    
   
    if 'Overall' in data.keys():
        df = data['Overall']

        df_splices = []
        overall_list = []
        for i in range(1, len(df['experimentVariant'].unique())):
            df_splices.append(df[df['experimentVariant'].isin({'0', str(i)})])
            df_inter = significance_df(df_splices[i-1])
            overall_list.append(df_inter)
            export_data[f'var{i}_Overall'] = df_inter
        
        ## Combining Different Variants of Overall Data
        export_data['Overall'] = pd.concat(overall_list).drop_duplicates()
        combined_data['Overall'] = pd.concat(overall_list).drop_duplicates()

    if 'Device' in data.keys():
        df = data['Device']
        deviceCalc(df)
    
    if 'User' in data.keys():
        df = data['User']
        userCalc(df)
               
    if 'DeviceUser' in data.keys():
        df = data['DeviceUser']
        deviceUserCalc(df)
       
    if 'Day' in data.keys():
        df = data['Day']
        dayCalc(df)

    if 'DayDevice' in data.keys():
        df = data['DayDevice']
        dayDeviceCalc(df)
    
    ## Combining Different Variants of Device Data
    try:
        combined_data['Device_desktop'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('Device_desktop')]).drop_duplicates()
    except:
        print('No desktop Data')
    try:
        combined_data['Device_mobile'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('Device_mobile')]).drop_duplicates()
    except:
        print('No mobile Data')
    
    try:
        combined_data['Device_tablet'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('Device_tablet')]).drop_duplicates()
    except:
        print('No tablet Data')

    ## Combining Different Variants of Visitor Data

    try:
        combined_data['User_New Visitor'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('User_New Visitor')]).drop_duplicates()
    except:
        print('No desktop Data')
    try:
        combined_data['User_Returning Visitor'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('User_Returning Visitor')]).drop_duplicates()
    except:
        print('No mobile Data')
        
    ## Combining Different Variants of Visitor Device Data

    try:
        combined_data['Desktop_New Visitor'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('desktop_New Visitor')]).drop_duplicates()
    except:
        print('No desktop Data')
    try:
        combined_data['Mobile_New Visitor'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('mobile_New Visitor')]).drop_duplicates()
    except:
        print('No mobile Data')
    try:
        combined_data['Tablet_New Visitor'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('tablet_New Visitor')]).drop_duplicates()
    except:
        print('No tablet Data')

    try:
        combined_data['Desktop_Returning Visitor'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('desktop_Returning Visitor')]).drop_duplicates()
    except:
        print('No desktop Data')
    try:
        combined_data['Mobile_Returning Visitor'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('mobile_Returning Visitor')]).drop_duplicates()
    except:
        print('No mobile Data')
    try:
        combined_data['Tablet_Returning Visitor'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('tablet_Returning Visitor')]).drop_duplicates()
    except:
        print('No tablet Data')


    # Combine Device Variant Level Daily Data

    try:
        export_graphs['Daily_Cummulative_desktop'] = pd.merge(export_graphs['Var1_Daily_Cummulative_desktop'],export_graphs['Var2_Daily_Cummulative_desktop'], on ='date')
    except:
        print('No Desktop Data')    
    try:
        export_graphs['Daily_Cummulative_mobile'] = pd.merge(export_graphs['Var1_Daily_Cummulative_mobile'],export_graphs['Var2_Daily_Cummulative_mobile'], on ='date')
    except:
        print('No Mobile Data')    
    try:
        export_graphs['Daily_Cummulative_tablet'] = pd.merge(export_graphs['Var1_Daily_Cummulative_tablet'],export_graphs['Var2_Daily_Cummulative_tablet'], on ='date')
    except:
        print('No Tablet Data')
        
    return export_data, export_graphs,combined_data

def data_significance(data):
    export_data = {}
    
    if 'Overall' in data.keys():
        df = data['Overall']

        df_splices = []
        overall_list = []
        for i in range(1, len(df['experimentVariant'].unique())):
            df_splices.append(df[df['experimentVariant'].isin({'0', str(i)})])
            df_inter = significance_df(df_splices[i-1])
            overall_list.append(df_inter)
            export_data[f'var{i}_Overall'] = df_inter
        
        export_data['Overall'] = pd.concat(overall_list).drop_duplicates()

    if 'Device' in data.keys():
        df = data['Device']
        
        df_splices = []
        for i in range(1, len(df['experimentVariant'].unique())):
            df_splices.append(df[df['experimentVariant'].isin({'0', str(i)})])
            for j in df_splices[i-1]['deviceCategory'].unique():
                df_spliced = df_splices[i-1][df_splices[i-1]['deviceCategory'] == j]
                if len(df_spliced) == 2:
                    export_data[f'var{i}_Device_{j}'] = significance_df(df_spliced)
                else:
                    print(f"No {j} Device")

            for comb in combinations(df['deviceCategory'].unique(), 2):
                df_int = df[df['deviceCategory'].isin(comb)]
                df_int['sessions'] = df_int['sessions'].astype(int)
                df_int['users'] = df_int['users'].astype(int)
                df_int['transactions'] = df_int['transactions'].astype(int)
                df_int['transactionRevenue'] = df_int['transactionRevenue'].astype(float)
                df_mix = df_int.groupby('experimentVariant')['sessions', 'users', 'transactions','transactionRevenue'].sum().reset_index()
                df_mix['experimentName'] = df_int['experimentName'].unique()[0]
                df_mix['revenuePerTransaction'] = df_mix['transactionRevenue']/df_mix['transactions']
                df_mix['transactionsPerSession'] = (df_mix['transactions']/df_mix['sessions'])*100
                export_data[f"var{i}_Device_{'_'.join(comb)}"] = significance_df(df_mix)

        try:
            export_data['Device_desktop'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('Device_desktop')]).drop_duplicates()
        except:
            print('No desktop Data')
        try:
            export_data['Device_mobile'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('Device_mobile')]).drop_duplicates()
        except:
            print('No mobile Data')
        
        try:
            export_data['Device_tablet'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('Device_tablet')]).drop_duplicates()
        except:
            print('No tablet Data')
    
    
    if 'User' in data.keys():
        df = data['User']

        df_splices = []
        for i in range(1, len(df['experimentVariant'].unique())):
            df_splices.append(df[df['experimentVariant'].isin({'0', str(i)})])
            for j in df_splices[i-1]['userType'].unique():
                df_spliced = df_splices[i-1][df_splices[i-1]['userType'] == j]
                if len(df_spliced) == 2:
                    export_data[f'var{i}_User_{j}'] = significance_df(df_spliced)
                else:
                    print(f"No {j} User")
        try:
            export_data['User_New Visitor'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('User_New Visitor')]).drop_duplicates()
        except:
            print('No desktop Data')
        try:
            export_data['User_Returning Visitor'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('User_Returning Visitor')]).drop_duplicates()
        except:
            print('No mobile Data')
        
    if 'DeviceUser' in data.keys():
        df = data['DeviceUser']

        df_splices = []
        for i in range(1, len(df['experimentVariant'].unique())):
            df_splices.append(df[df['experimentVariant'].isin({'0', str(i)})])
            df_dev_usr = df_splices[i-1]
            for u in df_dev_usr['userType'].unique():
                for d in df_dev_usr['deviceCategory'].unique():
                    df_spliced = df_dev_usr[(df_dev_usr['userType'] == u) & (df_dev_usr['deviceCategory'] == d)]
                    if len(df_spliced) == 2:
                        export_data[f'var{i}_{d}_{u}'] = significance_df(df_spliced)
                    else:
                        print("Not enough Data")

        try:
            export_data['Desktop_New Visitor'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('desktop_New Visitor')]).drop_duplicates()
        except:
            print('No desktop Data')
        try:
            export_data['Mobile_New Visitor'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('mobile_New Visitor')]).drop_duplicates()
        except:
            print('No mobile Data')
        try:
            export_data['Tablet_New Visitor'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('tablet_New Visitor')]).drop_duplicates()
        except:
            print('No tablet Data')

        try:
            export_data['Desktop_Returning Visitor'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('desktop_Returning Visitor')]).drop_duplicates()
        except:
            print('No desktop Data')
        try:
            export_data['Mobile_Returning Visitor'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('mobile_Returning Visitor')]).drop_duplicates()
        except:
            print('No mobile Data')
        try:
            export_data['Tablet_Returning Visitor'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('tablet_Returning Visitor')]).drop_duplicates()
        except:
            print('No tablet Data')

    if 'Credit' in data.keys():
        df = data['Credit']

        df_splices = []
        for i in range(1, len(df['experimentVariant'].unique())):
            df_splices.append(df[df['experimentVariant'].isin({'0', str(i)})])
            df_dev_usr = df_splices[i-1]
            for u in df_dev_usr['userFinancePreference'].unique():
                for d in df_dev_usr['deviceCategory'].unique():
                    df_spliced = df_dev_usr[(df_dev_usr['userFinancePreference'] == u) & (df_dev_usr['deviceCategory'] == d)]
                    if len(df_spliced) == 2:
                        export_data[f'var{i}_{d}_{u}'] = significance_df(df_spliced)
                    else:
                        print("Not enough Data")

        try:
            export_data['Desktop_Cash'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('desktop_Cash')]).drop_duplicates()
        except:
            print('No desktop Data')
        try:
            export_data['Mobile_Cash'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('mobile_Cash')]).drop_duplicates()
        except:
            print('No mobile Data')
        try:
            export_data['Tablet_Cash'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('tablet_Cash')]).drop_duplicates()
        except:
            print('No tablet Data')

        try:
            export_data['Desktop_Credit'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('desktop_Credit')]).drop_duplicates()
        except:
            print('No desktop Data')
        try:
            export_data['Mobile_Credit'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('mobile_Credit')]).drop_duplicates()
        except:
            print('No mobile Data')
        try:
            export_data['Tablet_Credit'] = pd.concat([export_data[i] for i in export_data.keys() if i.endswith('tablet_RCredit')]).drop_duplicates()
        except:
            print('No tablet Data')

      
    export_graphs = {}
    
    if 'Day' in data.keys():
        df = data['Day']
        if len(df) > 1:
            df['sessions'] = df['sessions'].astype(int)
            df['transactions'] = df['transactions'].astype(int)
            df['experimentVariant'] = df['experimentVariant'].astype(str)
            df['date'] = df['date'].replace('-','', regex=True).astype(int)
            if len(df['experimentVariant'].unique()) ==2 :
                export_graphs['Daily_Cummulative'] = control_variant(df)

            if len(df['experimentVariant'].unique()) > 2 :
                df_splice1 = df[df['experimentVariant'].isin({'0','1'})]
                export_graphs['var1_Daily_Cummulative'] = control_variant(df_splice1)
                
                df_splice2 = df[df['experimentVariant'].isin({'0','2'})]
                df_splice2['experimentVariant'] = df_splice2['experimentVariant'].replace('2','1')
                export_graphs['var2_Daily_Cummulative'] = control_variant(df_splice2)


    if 'DayDevice' in data.keys():
        print('DayDevice')
        if len(df) > 1:
            df = data['DayDevice']
            df['sessions'] = df['sessions'].astype(int)
            df['transactions'] = df['transactions'].astype(int)
            df['experimentVariant'] = df['experimentVariant'].astype(str)
            df['date'] = df['date'].replace('-','', regex=True).astype(int)
            if len(df['experimentVariant'].unique()) == 2 :
                for i in df['deviceCategory'].unique():
                    df_spliced = df[df['deviceCategory'] == i]
                    export_graphs[f'Daily_Cummulative_{i}'] = control_variant(df_spliced)
            
            if len(df['experimentVariant'].unique()) > 2 :
                df_splice1 = df[df['experimentVariant'].isin({'0','1'})]
                for i in df_splice1['deviceCategory'].unique():
                    df_spliced = df_splice1[df_splice1['deviceCategory'] == i]
                    export_graphs[f'Var1_Daily_Cummulative_{i}'] = control_variant(df_spliced)
                    
                df_splice2 = df[df['experimentVariant'].isin({'0','2'})]
                df_splice2['experimentVariant'] = df_splice2['experimentVariant'].replace('2','1')
                for i in df_splice2['deviceCategory'].unique():
                    df_spliced = df_splice2[df_splice2['deviceCategory'] == i]
                    export_graphs[f'Var2_Daily_Cummulative_{i}'] = control_variant(df_spliced)

        try:
            export_graphs['Daily_Cummulative_desktop'] = pd.merge(export_graphs['Var1_Daily_Cummulative_desktop'],export_graphs['Var2_Daily_Cummulative_desktop'], on ='date')
        except:
            print('No Desktop Data')    
        try:
            export_graphs['Daily_Cummulative_mobile'] = pd.merge(export_graphs['Var1_Daily_Cummulative_mobile'],export_graphs['Var2_Daily_Cummulative_mobile'], on ='date')
        except:
            print('No Mobile Data')    
        try:
            export_graphs['Daily_Cummulative_tablet'] = pd.merge(export_graphs['Var1_Daily_Cummulative_tablet'],export_graphs['Var2_Daily_Cummulative_tablet'], on ='date')
        except:
            print('No Tablet Data')    


    return export_data,export_graphs

def uplift_calculation(df, total_users, startDay, endDay):
    test_users = df['users'].sum()
    days_expt = datetime.strptime(endDay, '%Y-%m-%d') - datetime.strptime(startDay, '%Y-%m-%d')

    df_control = df[df['experimentVariant'] == 0].reset_index(drop=True)
    df_variant = df[df['experimentVariant'] != 0].reset_index(drop=True)

    endDate = datetime.strptime(endDay, '%Y-%m-%d') 
    daysinMonth = monthrange(endDate.year,endDate.month)[1] 

    users_per_month = (total_users/int(days_expt.days))*daysinMonth
    perc_customers = (test_users/total_users)
    perc_100_variation = perc_customers*users_per_month

    revenue_control =( (df_control['User Conversion Rate'][0]/100)*perc_100_variation)* df_control['revenuePerTransaction'][0]
    revenue_variration =((df_variant['User Conversion Rate'][0]/100)*perc_100_variation) * df_variant['revenuePerTransaction'][0]
    uplift = ((revenue_variration - revenue_control)/revenue_control)

    

    return {"Chance Of Being Best": df_variant['Chance_Of_Being_Best'][0],
            "Change in Revenue": (revenue_variration - revenue_control)*12, 
            "Uplift" :  uplift*100}

def dataframeImage(df, header = False):
    df['Revenue Per User'] = df['transactionRevenue']/ df['users']
    
    
    df = df[['experimentVariant', 'sessions', 'users', 
            'transactions','transactionRevenue','revenuePerTransaction','Revenue Per User',
            'E-Commerce Conversion Rate',
            'ECR_Change','User Conversion Rate', 'UCR_Change', 'Chance_Of_Being_Best']]
    
    df = df.rename({'experimentVariant':"Experiment Variant", 
                    'sessions':'Sessions',
                    'users':"Users",
                    'transactions':'Transactions',
                    'transactionRevenue':'Revenue',
                    'revenuePerTransaction':'AOV',
                    'Revenue Per User':"RpU",
                    'E-Commerce Conversion Rate':'ECR',
                    'ECR_Change':'ECR Change',
                    'User Conversion Rate':'UCR',
                    'UCR_Change':'UCR Change',
                    'Chance_Of_Being_Best':'Chance Of Being Best'}, axis = 1)

    df['Sessions'] = df['Sessions'].apply(lambda x:'{:20,}'.format(x))
    df['Users'] = df['Users'].apply(lambda x:'{:20,}'.format(x))
    df['Transactions'] = df['Transactions'].apply(lambda x:'{:20,}'.format(x))
    df['Revenue'] = df['Revenue'].apply(lambda x:'\xA3'+'{:20,.2f}'.format(x))
    df['AOV'] = df['AOV'].apply(lambda x:'\xA3'+'{:20,.2f}'.format(x))
    df['RpU'] = df['RpU'].apply(lambda x:'\xA3'+'{:20,.2f}'.format(x))
    df['ECR'] = df['ECR'].apply(lambda x:'{:20,.2f}'.format(x)+'%')
    df['ECR Change'] = df['ECR Change']*100
    df['ECR Change'] = df['ECR Change'].apply(lambda x:'{:20,.2f}'.format(x)+'%') 
    df['UCR Change'] = df['UCR Change']*100
    df['UCR Change'] = df['UCR Change'].apply(lambda x:'{:20,.2f}'.format(x)+'%') 
    
    def zeroToNull(x):
        x = x.replace(' ','')
        if x.startswith('0.00%'):
            x = '-'
        return x
    

    df['UCR'] = df['UCR'].apply(lambda x:'{:20,.2f}'.format(x)+'%')
    df['Chance Of Being Best'] = df['Chance Of Being Best'].apply(lambda x:'{:20,.2f}'.format(x)+'%')
    df.sort_values('Experiment Variant', inplace=True)
    
    
    df['UCR Change'] = df['UCR Change'].apply(zeroToNull)
    df['ECR Change'] = df['ECR Change'].apply(zeroToNull)
    df['Chance Of Being Best'] = df['Chance Of Being Best'].apply(zeroToNull)


    df['Experiment Variant'] = df['Experiment Variant'].replace(0,'Control')
    df['Experiment Variant'] = df['Experiment Variant'].replace(1,'Challenger 1')
    df['Experiment Variant'] = df['Experiment Variant'].replace(2,'Challenger 2')
    df = df.rename({"Experiment Variant":""}, axis = 1)
    
    if header == False:
        df_styled= df.round(2).astype(str).reset_index(drop=True).style.set_table_styles(
               [{'selector': 'th',                           
                   'props': [('background-color', 'white'),('color', 'white')]
               }]).set_properties(**{'background-color': "white"}).hide_index()
    else:
         df_styled= df.round(2).astype(str).reset_index(drop=True).style.set_table_styles(
               [{'selector': 'th',                           
                   'props': [('background-color', 'white')]
               }]).set_properties(**{'background-color': "white"}).hide_index()
        
    
    return df_styled

def graphingCummulative(graphData,issueId):
    for i in graphData.keys():
        print(i)
        desktop_daily = graphData[i]
        try:
            desktop_daily['date'] = desktop_daily['date'].apply(lambda x:datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S"))
        except:
            desktop_daily['date'] = desktop_daily['date'].apply(lambda x:datetime.strptime(str(x), "%Y%m%d"))

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=desktop_daily['date'], y=desktop_daily['Control'],line_color='rgb(0,0,0)',
                            mode='lines',
                            name='Control'))
        fig.add_trace(go.Scatter(x=desktop_daily['date'], y=desktop_daily['variant'],
                            mode='lines',line_color='rgb(37,208,58)',
                            name='Challenger'))

        fig.update_yaxes(ticksuffix="%")
        fig.update_xaxes(showline=True, linewidth=.5, linecolor='black', mirror=True)
        fig.update_yaxes(showline=True, linewidth=.5, linecolor='black', mirror=True)


        fig.update_layout(autosize=False,
            width=1418,
            height=550,
            legend=dict(
            yanchor="bottom",
            y=0.01,
            xanchor="right",
            x=0.98,
            bgcolor="rgba(0,0,0,0)"),
            plot_bgcolor='rgba(0,0,0,0)')

       
        fig.write_image(f"./Template/assets/images/{issueId}/graphs/{i}.jpeg")

def graphingCummulative2(graphData,issueId):
    for i in graphData.keys():
        if i.startswith('Daily_Cummulative'):

            print(i)
            desktop_daily = graphData[i]
            try:
                desktop_daily['date'] = desktop_daily['date'].apply(lambda x:datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S"))
            except:
                desktop_daily['date'] = desktop_daily['date'].apply(lambda x:datetime.strptime(str(x), "%Y%m%d"))

            fig = go.Figure()
            fig.add_trace(go.Scatter(x=desktop_daily['date'], y=desktop_daily['Control_x'],line_color='rgb(0,0,0)',
                                mode='lines',
                                name='Control'))
            fig.add_trace(go.Scatter(x=desktop_daily['date'], y=desktop_daily['variant_x'],
                                mode='lines',line_color='rgb(37,208,58)',
                                name='Challenger_1'))
            fig.add_trace(go.Scatter(x=desktop_daily['date'], y=desktop_daily['variant_y'],
                                mode='lines',line_color='rgb(20,100,60)',
                                name='Challenger_2'))


            fig.update_yaxes(ticksuffix="%")
            fig.update_xaxes(showline=True, linewidth=.5, linecolor='black', mirror=True)
            fig.update_yaxes(showline=True, linewidth=.5, linecolor='black', mirror=True)


            fig.update_layout(autosize=False,
                width=1418,
                height=550,
                legend=dict(
                yanchor="bottom",
                y=0.01,
                xanchor="right",
                x=0.98,
                bgcolor="rgba(0,0,0,0)"),
                plot_bgcolor='rgba(0,0,0,0)')

    

            fig.write_image(f"./Template/assets/images/{issueId}/graphs/{i}.jpeg")

def typeofExperiment(ticket):
    multiDevice = False
    singleData = False
    noData = False
    if len(ticket['Devices']) == 3:
        multiDevice = True
        if ((ticket['desktoptestTitle'] is not None) & (ticket['mobiletestTitle'] is not None) & (ticket['tablettestTitle'] is not None)):
            singleData = False
        elif ticket['desktoptestTitle'] is not None:
            singleData = True
            startDate = ticket[f"desktopliveDate"]
            endDate = ticket[f"desktopendDate"]
            title = ticket[f"desktoptestTitle"]
            testId = ticket[f"desktoptestId"]
            controlId = ticket[f"desktopcontrolId"]
            variationId = ticket[f"desktopvariationIds"]
            dimension = ticket[f"desktopdimension"]
        else:
            noData = True         
            
    elif len(ticket['Devices']) == 2:
        multiDevice = True
        a = ticket[f"{ticket['Devices'][0].lower()}testTitle"]
        b = ticket[f"{ticket['Devices'][1].lower()}testTitle"]

        if (a is not None) & (b is not None):
            singleData = False
        else:
            
            if a is not None:
                singleData = True
                startDate = ticket[f"{ticket['Devices'][0].lower()}liveDate"]
                endDate = ticket[f"{ticket['Devices'][0].lower()}endDate"]
                title = ticket[f"{ticket['Devices'][0].lower()}testTitle"]
                testId = ticket[f"{ticket['Devices'][0].lower()}testId"]
                controlId = ticket[f"{ticket['Devices'][0].lower()}controlId"]
                variationId = ticket[f"{ticket['Devices'][0].lower()}variationIds"]
                dimension = ticket[f"{ticket['Devices'][0].lower()}dimension"]
            elif b is not None:
                singleData = True
                startDate = ticket[f"{ticket['Devices'][1].lower()}liveDate"]
                endDate = ticket[f"{ticket['Devices'][1].lower()}endDate"]
                title = ticket[f"{ticket['Devices'][1].lower()}testTitle"]
                testId = ticket[f"{ticket['Devices'][1].lower()}testId"]
                controlId = ticket[f"{ticket['Devices'][1].lower()}controlId"]
                variationId = ticket[f"{ticket['Devices'][1].lower()}variationIds"]
                dimension = ticket[f"{ticket['Devices'][1].lower()}dimension"]
            else:
                noData = True

    elif len(ticket['Devices']) == 1:
        singleData = True
        startDate = ticket[f"{ticket['Devices'][0].lower()}liveDate"]
        endDate = ticket[f"{ticket['Devices'][0].lower()}endDate"]
        title = ticket[f"{ticket['Devices'][0].lower()}testTitle"]
        testId = ticket[f"{ticket['Devices'][0].lower()}testId"]
        controlId = ticket[f"{ticket['Devices'][0].lower()}controlId"]
        variationId = ticket[f"{ticket['Devices'][0].lower()}variationIds"]
        dimension = ticket[f"{ticket['Devices'][0].lower()}dimension"]
        
    else:
        print('No Device Level Information')



    if singleData :
        if multiDevice:
            data = {'startDate':startDate,
                    'endDate':endDate,
                    'title':title,
                    'testId':testId,
                    'controlId':controlId,
                    'variationId':variationId,
                    'dimension':dimension}
            
            return(singleData, multiDevice, noData,data)
        else:
            data = {'startDate':startDate,
                    'endDate':endDate,
                    'title':title,
                    'testId':testId,
                    'controlId':controlId,
                    'variationId':variationId,
                    'dimension':dimension}
            
            return(singleData, multiDevice, noData,data)
            print('SingleDevice')
    else:
        if not noData:
            data = None
            return(singleData, multiDevice, noData,data)
        else:
            return (None,None,noData,None)

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

def finalResult(df):
    df['inter'] = df[''].apply(lambda x: len(x.split(' ')))
    df['Chance of Being Best']  = df['Chance of Being Best'].str.replace('%','').astype('float')
    df['Revenue Uplift'] = df['Revenue Uplift'].apply(lambda x:x.split('.')[0])
    df['UCR Change'] = df['UCR Change'].apply(lambda x:x.split('%')[0]).astype(float).round(1).apply(lambda x:str(x)+'%')

    multiple_variants = False
    if df[''].str.contains('var2').sum() > 0:
        multiple_variants = True

    df_greaterthan90 =  df[(df['inter'] == 2) & (df['Chance of Being Best'] > 90)]

    overall_result = False
    desktop_result = False
    mobile_result = False
    tablet_result = False
    if len(df_greaterthan90) > 0 :
        if df_greaterthan90[''].str.contains('Overall').sum() > 0:
            df_inter = df_greaterthan90[df_greaterthan90[''].str.contains('Overall')]
            if len(df_inter) == 1:
                overall_result = True
                print('One Overall Winner')
                values = df_inter.iloc[0]
                Overall_variant = values[''] 
                Overall_chance = values['Chance of Being Best']
                Overall_UCR_Change = values['UCR Change']
                Overall_Uplift = values['Uplift']
                Overall_Revenue =  values['Revenue Uplift']

            else:
                overall_result = True
                print('Multiple Overall Winner')
                values = df_inter.iloc[0]
                Overall_variant = values[''] 
                Overall_chance = values['Chance of Being Best']
                Overall_UCR_Change = values['UCR Change']
                Overall_Uplift = values['Uplift']
                Overall_Revenue =  values['Revenue Uplift']
        else:
            if df_greaterthan90[''].str.contains('Desktop').sum() >0:
                df_inter_d = df_greaterthan90[df_greaterthan90[''].str.contains('Desktop')]
                if len(df_inter_d) == 1:
                    desktop_result = True
                    print('One Desktop Winner')
                    values = df_inter_d.iloc[0]
                    desktop_variant = values['']
                    desktop_chance = values['Chance of Being Best']
                    desktop_UCR_Change = values['UCR Change']
                    desktop_Uplift = values['Uplift']
                    desktop_Revenue =  values['Revenue Uplift']

                else:
                    desktop_result = True
                    print('Multiple Desktop Winner')
                    values = df_inter_d.iloc[0]
                    desktop_variant = values['']
                    desktop_chance = values['Chance of Being Best']
                    desktop_UCR_Change = values['UCR Change']
                    desktop_Uplift = values['Uplift']
                    desktop_Revenue =  values['Revenue Uplift']


            if df_greaterthan90[''].str.contains('Mobile').sum() >0:
                df_inter_m = df_greaterthan90[df_greaterthan90[''].str.contains('Mobile')]
                if len(df_inter_m) == 1:
                    mobile_result = True
                    print('One Mobile Winner')
                    values = df_inter_m.iloc[0]
                    mobile_variant = values['']
                    mobile_chance = values['Chance of Being Best']
                    mobile_UCR_Change = values['UCR Change']
                    mobile_Uplift = values['Uplift']
                    mobile_Revenue =  values['Revenue Uplift']
                else:
                    mobile_result = True
                    print('Multiple Mobile Winner')
                    values = df_inter_m.iloc[0]
                    mobile_variant = values['']
                    mobile_chance = values['Chance of Being Best']
                    mobile_UCR_Change = values['UCR Change']
                    mobile_Uplift = values['Uplift']
                    mobile_Revenue =  values['Revenue Uplift']

            if df_greaterthan90[''].str.contains('Tablet').sum() >0:
                df_inter_t = df_greaterthan90[df_greaterthan90[''].str.contains('Tablet')]
                if len(df_inter_t) == 1:
                    tablet_result = True
                    print('One Tablet Winner')
                    values = df_inter_t.iloc[0]
                    tablet_variant = values['']
                    tablet_chance = values['Chance of Being Best']
                    tablet_UCR_Change = values['UCR Change']
                    tablet_Uplift = values['Uplift']
                    tablet_Revenue =  values['Revenue Uplift']
                else:
                    tablet_result = True
                    print('Multiple Tablet Winner')
                    values = df_inter_t.iloc[0]
                    tablet_variant = values['']
                    tablet_chance = values['Chance of Being Best']
                    tablet_UCR_Change = values['UCR Change']
                    tablet_Uplift = values['Uplift']
                    tablet_Revenue =  values['Revenue Uplift']

            df_overall = df[(df['inter'] == 2)]
            if sum(df_overall[''].str.contains('Overall')) > 0:
                df_overall = df_overall[df_overall[''].str.contains('Overall')]
                values = df_overall.iloc[0]
                Overall_chance = values['Chance of Being Best']
                Overall_UCR_Change = values['UCR Change']
            else:
                values = df_overall.iloc[0]
                Overall_chance = values['Chance of Being Best']
                Overall_UCR_Change = values['UCR Change']
    else:
        print('No Winner')
        df_overall = df[(df['inter'] == 2)]
        if sum(df_overall[''].str.contains('Overall')) > 0:
            df_overall = df_overall[df_overall[''].str.contains('Overall')]
            values = df_overall.iloc[0]
            Overall_chance = values['Chance of Being Best']
            Overall_UCR_Change = values['UCR Change']
        else:
            values = df_overall.iloc[0]
            Overall_chance = values['Chance of Being Best']
            Overall_UCR_Change = values['UCR Change']

    
    
    if overall_result:
        result = 'During the observed period Challenger overperformed Control.'
        table_result = f'<ul style="list-style-type:none;"> <li> <b>{Overall_UCR_Change}</b> UCR change against Control</li> <li> <b>{Overall_chance}</b>% chance of being better than the Control</li> <li> <b> {Overall_Revenue} </b> forecasted revenue uplift over 12 months</li>    </ul>'
        result = result + table_result
    else:
        if Overall_chance < 20:
            result = 'During the observed period Challenger did not overperform Control.'
            table_result = f'<ul style="list-style-type:none;"> <li> <b>{Overall_UCR_Change}</b> UCR change against Control</li> <li> <b>{Overall_chance}</b>% chance of being better than the Control</li>    </ul>'
            result = result + table_result

        else:
            result = 'During the observed period Challenger did not overperform Control.'
            table_result = f'<ul style="list-style-type:none;"> <li> <b>{Overall_UCR_Change}</b> UCR change against Control</li> <li> <b>{Overall_chance}</b>% chance of being better than the Control</li>  </ul>'
            result = result + table_result

        if desktop_result | mobile_result | tablet_result:

            device_text = ''
            if desktop_result:
                table_desktop = f'<ul style="list-style-type:none;"> <li> <b>{desktop_UCR_Change}</b> UCR change against Control</li> <li> <b>{desktop_chance}</b> chance of being better than the Control</li> <li> <b>{desktop_Revenue} </b> forecasted revenue uplift over 12 months</li>    </ul>'

                if multiple_variants:
                    var_val_d = desktop_variant.split(' ')[0][-1]
                    variant_d = 'Challenger '+var_val_d + table_desktop
                else:
                    variant_d = ''
                device_text = '<b>Desktop </b>' + variant_d + table_desktop

            if mobile_result:
                table_mobile = f'<ul style="list-style-type:none;"> <li> <b>{mobile_UCR_Change}</b> UCR change against Control</li> <li> <b>{mobile_chance}</b> chance of being better than the Control</li> <li><b> {mobile_Revenue}</b> forecasted revenue uplift over 12 months </li>    </ul>'

                if multiple_variants:
                    var_val_m = mobile_variant.split(' ')[0][-1]
                    variant_m = 'Challenger '+ var_val_m + table_mobile
                else:
                    variant_m = ''

                if len(device_text) > 0:
                    device_text = device_text+ '</b> Mobile </b>' + variant_m + table_mobile
                else: 
                    device_text = '<b>Mobile </b>'  + variant_m


            if tablet_result:
                table_tablet = f'<ul style="list-style-type:none;"> <li> <b>{tablet_UCR_Change}</b> UCR change against Control</li> <li> <b>{tablet_chance}</b> chance of being better than the Control</li> <li><b>  {tablet_Revenue}</b> forecasted revenue uplift over 12 months </li>    </ul>'


                if multiple_variants:
                    var_val_t = tablet_variant.split(' ')[0][-1]
                    variant_t = 'Challenger '+ var_val_t + table_tablet
                else:
                    variant_t = ''


                if len(device_text) > 0:
                    device_text = device_text+ '<b>Tablet </b> '  + variant_t + table_tablet
                else:
                    device_text = '<b>Tablet </b>'  + variant_t

            result = result  +'\n' + 'However, Challenger overperformed Control on ' +'\n' + device_text 
        
    return result

def reportMail(ticket, mail_ids):
    JIRA_ticket = ticket['Title']
    mail_ids = mail_ids.split(';')
    recipients_emails = mail_ids

    smtp_host = 'smtp-mail.outlook.com'
    login, password = 'akhil.vsrp@endlessgain.com','Ak@1994hil'
    
    msg = MIMEMultipart()
    msg.attach(MIMEText(f'Please find the attached report for {JIRA_ticket}', "plain"))
    msg['Subject'] = Header(f'{JIRA_ticket} - Report', 'utf-8')
    msg['From'] = login
    msg['To'] = ", ".join(recipients_emails)
    
        # Attach the pdf to the msg going by e-mail
    with open( f"./PDF/{ticket['Title']}.pdf", "rb") as f:
        #attach = email.mime.application.MIMEApplication(f.read(),_subtype="pdf")
        attach = MIMEApplication(f.read(),_subtype="pdf")
    attach.add_header('Content-Disposition','attachment',filename=str(ticket['Title']+".pdf"))
    msg.attach(attach)
    s = smtplib.SMTP(smtp_host, 587, timeout=30)
    # s.set_debuglevel(1)
    try:
        s.starttls()
        s.login(login, password)
        s.sendmail(msg['From'], recipients_emails, msg.as_string())
    finally:
        s.quit()
    
 