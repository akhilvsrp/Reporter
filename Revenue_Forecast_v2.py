# -*- coding: utf-8 -*-
"""
Created on Fri Oct 23 12:18:51 2020

@author: PoojaBhat
"""

import pandas as pd
from pandas import datetime
from pmdarima.arima import auto_arima
import numpy as np
from statsmodels.tsa.stattools import adfuller
from matplotlib import pyplot
import plotly.graph_objects as go
from scipy.special import boxcox, inv_boxcox
from dateutil import relativedelta
from plotly.offline import plot

#ADF Test
def check_stationarity(data):
    data=data.dropna()
    dftest = adfuller(data, autolag='AIC')
    dfoutput = pd.Series(dftest[0:4], index=['Test Statistic','p-value','#Lags Used','Number of Observations Used'])
    for key,value in dftest[4].items():
       dfoutput['Critical Value (%s)'%key] = value
    return (dfoutput)

#Get the best transformation
def data_transaformations(data):
    min_test_stat=0
    best_transformed_data=data
    best_transform=''
    
    #Original data
    original_test_res=check_stationarity(data)
    min_test_stat=original_test_res[0]
    best_transform='Original'
    
    #Differenced data
    diff_data=data-data.shift(1)
    diff_data_test_res=check_stationarity(diff_data)
    if(diff_data_test_res[0]<min_test_stat):
        min_test_stat=diff_data_test_res[0]
        best_transformed_data=diff_data
        best_transform='Diff'
        
    #Log Transform
    log_data=np.log(data)
    log_data_test_res=check_stationarity(log_data)
    if(log_data_test_res[0]<min_test_stat):
        min_test_stat=log_data_test_res[0]
        best_transformed_data=log_data
        best_transform='Log'
        
    #Log Transform and Differencing
    ld_data=np.log(data)-np.log(data).shift(1)
    ld_data_test_res=check_stationarity(ld_data)
    if(ld_data_test_res[0]<min_test_stat):
        min_test_stat=ld_data_test_res[0]
        best_transformed_data=ld_data
        best_transform='LD'
     
    #Box - Cox Transformation
    box_data=boxcox(data,2.5)
    box_data_test_res=check_stationarity(box_data)
    if(box_data_test_res[0]<min_test_stat):
        min_test_stat=box_data_test_res[0]
        best_transformed_data=box_data
        best_transform='Box Cox'
    
    return min_test_stat,best_transformed_data,best_transform
  
#Build Model  
def build_model(data,forecast_len):
    
    data=data.dropna()
    
    model=auto_arima(data,start_p=0,start_q=0,max_p=5,max_d=5,
                     start_P=0,start_Q=0,max_P=5,max_D=5,max_Q=5,m=12,
                     seasonal=True,trace=True,error_ection='warn',supress_warnings=True,stepwise=True,n_fits=50)    
   
    predictions=pd.DataFrame(model.predict(forecast_len))

    return predictions

#Reconstruct Data
def reverse_transform(train_data,transformed_data,transform_type):
    
    if(transform_type=='Original'):
        res_data=transformed_data
        
    if(transform_type=='Diff'):
        last_index=len(train_data)-1
        last_value=train_data.iloc[last_index,0]
        transformed_data.iloc[0,0]=last_value+transformed_data.iloc[0,0]
        for i in range(1,len(transformed_data)):
            transformed_data.iloc[i,0]=transformed_data.iloc[i-1,0]+transformed_data.iloc[i,0]
        res_data= transformed_data
        
    if(transform_type=='Log'):
        res_data=np.exp(transformed_data)
        
    if(transform_type=='LD'):
        train_data=np.log(train_data)
        last_index=len(train_data)-1
        last_value=train_data.iloc[last_index,0]
        transformed_data.iloc[0,0]=last_value+transformed_data.iloc[0,0]
        for i in range(1,len(transformed_data)):
            transformed_data.iloc[i,0]=transformed_data.iloc[i-1,0]+transformed_data.iloc[i,0]
        res_data= np.exp(transformed_data) 
        
    if(transform_type=='Box Cox'):
        res_data=inv_boxcox(transformed_data,2.5)  
            
    return res_data


# Parse Date
def parser(x):
	return datetime.strptime(x, '%Y%m')

#Main Function
def revenue_forecast(data,uplift):
    #Outliers
    Q1 = data.quantile(0.25)
    Q3 = data.quantile(0.75)
    IQR = Q3 - Q1
    data=data[~((data < (Q1 - 1.5 * IQR))|(data > (Q3 + 1.5 * IQR))).any(axis=1)]
    
    #Plot Original Data
    data.plot()
    pyplot.show()
    
    #Check Stationarity
    min_test_stat,trans_data,trans=data_transaformations(data)
    
    #Plot transformed Data
    trans_data.plot()
    pyplot.show()
    
    """
    #Original Data Split
    train_size = int(len(data) * 0.80)
    test_size=int(len(data))-train_size
    or_train, or_test = data[0:train_size], data[train_size:]
    
    train = trans_data[0:train_size]
    """
    #Build Model
    model_res=build_model(trans_data,12)
    
    #Reconstruct Data
    reconstrcuted_data=reverse_transform(data,model_res,trans)
    
    result_df=pd.DataFrame()

    result_df["Predicted"]=reconstrcuted_data.loc[:,0].values
    result_df["Uplift"]=result_df["Predicted"]+(result_df["Predicted"]*uplift)
    
    #Plot Result
    result_df.plot()
    pyplot.show()
    
    annual_revenue=result_df['Predicted'].sum()   
    annual_uplift_revenue=result_df['Uplift'].sum()  
    diff=annual_uplift_revenue-annual_revenue  
    #result_df.to_excel('C:\\Endless Gain\\Reporter Project\\Codes\\TS Results\\Chums1.xlsx')
    return result_df,annual_revenue,annual_uplift_revenue,diff

def create_graph(data,result,location):
    #Final Data
    graph_data=data
    graph_data=graph_data.reset_index()
    graph_data['yearMonth']=graph_data['yearMonth'].dt.date
    
    graph_data = graph_data.iloc[-24:]

    result.loc[0,'yearMonth']=graph_data.iloc[[-1],0].values[0]+ relativedelta.relativedelta(months=1)
    for i in range(1,len(result)):
        result.loc[i,'yearMonth']=result.loc[i-1,'yearMonth'] + relativedelta.relativedelta(months=1)
    
    temp_df=graph_data.iloc[[-1],:]
    temp_df['Predicted']=temp_df['Uplift']=temp_df['transactionRevenue']
    
    temp_df=temp_df.append(result)
    graph_data=graph_data.append(temp_df)
    
    #Create Graph Components
    x=graph_data['yearMonth']
    
    #Lines
    y1=graph_data['transactionRevenue']
    y2=graph_data['Predicted']
    y2_lower=y2-(0.15*y2)
    y2_upper=y2+(0.15*y2)
    
    y3=graph_data['Uplift']
    y3_lower=y3-(0.15*y3)
    y3_upper=y3+(0.15*y3)
    
    layout= go.Layout(xaxis = {'showgrid': False},yaxis = {'showgrid': False})
    fig=go.Figure(layout=layout)
    

    fig.add_trace(go.Scatter(
        x=x, y=y1,
        line_color='rgb(0,0,0)',
        name='Historical',
    ))
    fig.add_trace(go.Scatter(
        x=x, y=y2,
        line_color='rgb(171,165,165)',
        name='Forecast'
    ))
    
    fig.add_trace(go.Scatter(
        x=x, y=y3,
        line_color='rgb(37,208,58)',
        #line=dict(color='rgb(37,208,58)', width=2, dash='dash'),
        name='Uplift'
    ))
    
    fig.add_trace(go.Scatter(
        x=x,
        y=y2_upper,
        #fill='toself',
        fillcolor='rgba(171,165,165,0.2)',
        line_color='rgba(255,255,255,0)',
        showlegend=False,
        name='Uplift',
    ))
    fig.add_trace(go.Scatter(
        x=x,
        y=y2_lower,
        fill='tonexty',
        fillcolor='rgba(171,165,165,0.2)',
        line_color='rgba(255,255,255,0)',
        showlegend=False,
        name='Uplift',
    ))
    """
    fig.add_trace(go.Scatter(
        x=x,
        y=y3_upper,
        #fill='toself',
        fillcolor='rgba(37,208,58,0.2)',
        line_color='rgba(255,255,255,0)',
        showlegend=False,
        name='Uplift',
    ))
    fig.add_trace(go.Scatter(
        x=x,
        y=y3_lower,
        fill='tonexty',
        fillcolor='rgba(37,208,58,0.2)',
        line_color='rgba(255,255,255,0)',
        showlegend=False,
        name='Uplift',
        
    ))
    """
    fig.update_traces(mode='lines')

    fig.update_layout(autosize=False,
            width=1418,
            height=550,
            legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.98,
            bgcolor="rgba(0,0,0,0)"),
            plot_bgcolor='rgba(0,0,0,0)')

    fig.write_image(f"./Template/assets/images/{location}/graphs/Revenue.jpeg")


#Function Call
# series = pd.read_csv('C:\\Users\\PoojaBhat\\Desktop\\Revenue_Data.csv', header=0, parse_dates=[0], index_col=0, squeeze=True, date_parser=parser)

# data=series.loc[(series['Client']=='Hotter') & series['transactionRevenue']!=0]
# data=data.drop(['Client'],axis=1)

# uplift=0.95



