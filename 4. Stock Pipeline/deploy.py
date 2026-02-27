#!/usr/bin/env python
# coding: utf-8

# ## Prefect Python Script

# In[2]:


from prefect import flow, task
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt
from datetime import date, timedelta
import yfinance as yf


# In[3]:


target = '2330'
market = 'TW'


# ## Data Preperation
# ### Download Data from yfinance

# In[5]:


# download data from yfinance
# target and market need to be strings
@task
def download(target,market):

        # end_day : doesn't included this day
        # +1 day to grab the data til today
        today = date.today()
        end_day = today + timedelta(days=1)

        # Create a start and end date
        start_day = end_day -timedelta(days=3*365)
       
        # Adjusting stock ticks with the market
        if market == 'TW' or market == 'Taiwan':
            target = target+'.TW'
        else :
            target = target
            
        # Download data from yfinance
        # set auto_adjust = False because I need unadjusted price
        download = yf.download(target,interval='1d',
                         start= start_day,end= end_day,group_by='ticker',
                        rounding=True,multi_level_index = False,auto_adjust=False);
    
        df = download.copy()
    
        # Drop adjusted close price column
        df.drop('Adj Close',axis=1,inplace=True)
        
        return df


# ### Resampling Data

# In[7]:


@task
def resample(df):
    # Resample to weekly and monthly data
    
    # Create the logic of resampling
    # Grab the open price of the first day in a week
    # Grab the highest High price of the week
    # Grab the lowest Low price of the week
    # Grab the close price of the last day in a week
    logic = {'Open'  : 'first',
             'High'  : 'max',
             'Low'   : 'min',
             'Close' : 'last',
             'Volume': 'sum'}
    
    # Drop NaN data
    # resample('W',default label ='right',default closed='right')
    # label='right' ensures the timestamp is the end of the period
    # closed='right' ensures the aggregation interval is inclusive of the right boundary (standard for 'W')
    wdf = df.resample('W').apply(logic).dropna()
    mdf = df.resample('M',label='right',closed='right').apply(logic).dropna()  

    return [df,wdf,mdf]


# ### Weekdays Identification

# In[9]:


@task
def weekdays(df,wdf,mdf):
     # Create a dictionary for mapping weekday
    weekdays = {0:"Monday",
            1:"Tuesday",
            2:"Wednesday",
            3:"Thursday",
            4:"Friday",
            5:"Saturday",
            6:"Sunday"}
    if df.index.inferred_type == 'datetime64' : 
        # Add weekday  
        df['Weekday'] = df.index.dayofweek.map(weekdays)
        wdf['Weekday'] = wdf.index.dayofweek.map(weekdays)
        mdf['Weekday'] = mdf.index.dayofweek.map(weekdays)
        
    else : 
        # Add weekday  
        df['Weekday'] = df['Date'].dt.dayofweek.map(weekdays)
        wdf['Weekday'] = wdf['Date'].dt.dayofweek.map(weekdays)
        mdf['Weekday'] = mdf['Date'].dt.dayofweek.map(weekdays)
    
    # if df doesn't have datetime index,use this code instead
    # df['Weekday'] = df['Date'].dt.dayofweek.map(weekdays)


# ### Creating Week_key

# In[11]:


@task
def week_key(df):
    # Create a list to store all keys
    keys = []
    
    # For loop iterating through each row
    for i in range(0,len(df)):
        
        # Extract the year and the week from the date
        year = df.loc[i,'Date'].year
        week = df.loc[i,'Date'].isocalendar().week

        # To create a 6-numbered week_key, need to add 0 to single digit number of week
        # Hence, add '0'
        if len(str(week)) ==1 :
            
            # Original issue : last few days(after 52 week) of a year could fall on the 1st week of the next year
            # week would roll back to 1, causing inaccurate week_key such as 202401 for 2024-12-30(Monday)
            # if the current year 52 week is existing in key, year+1 
            if week == 1 and (str(year)+ '52') in keys :
                df.loc[i,'week_key'] = str(year+1)+ '0' + str(week)
                
            else :
                df.loc[i,'week_key'] = str(year)+ '0' + str(week)
                keys.append(df.loc[i,'week_key'])
            
        else :
            df.loc[i,'week_key'] = str(year)+ str(week)
            keys.append(df.loc[i,'week_key'])
    


# ### Creating Month_key

# In[13]:


@task
def month_key(df):
    
    # For loop iterating through each row
    for i in range(0,len(df)):
        
        # Extract the year and the month from the date
        year = df.loc[i,'Date'].year
        month = df.loc[i,'Date'].month

        # To create a 6-numbered month_key, need to add 0 to single digit number of week
        # Hence, add '0'
        if len(str(month)) ==1 :
            df.loc[i,'month_key'] = str(year)+ '0' + str(month)
              
        else :
            df.loc[i,'month_key'] = str(year)+ str(month)


# ### Subflow for Data Preparation

# In[15]:


@flow
def data_prep (target,market):
   # target is your stock symbol/tick
# 1. Download the daily date
       
    # Execute download function
    df = download(target,market)
    
# 2. Resample to weekly and monthly data and add weekdays

    # Execute reample function
    [df,wdf,mdf] = resample(df)
    # Execute weekdays function
    weekdays(df,wdf,mdf)
   

# 3. Fixing date index by using week_key and month_key

    # Reset index so the function below can be performed (for loop,indexing or slicing)
    df.reset_index(inplace=True)
    wdf.reset_index(inplace=True)
    mdf.reset_index(inplace=True)

    
    # Create week_key for df and wdf
    # Execute week_key function
    week_key(df)
    week_key(wdf)

    # Create month_key for df and mdf
    # execute month_key function
    month_key(df)
    month_key(mdf)

    # Group df by week_key and month_key, and grab the last trading date of the week/month
    # remember to set as_index = False, otherwise week_key/month_key would be set as the index
    # Causing problem to assign these dates to index later
    week_index = df.groupby('week_key',as_index=False).last().sort_values('Date')['Date']
    month_index = df.groupby('month_key',as_index=False).last().sort_values('Date')['Date']

    # Assign the correct dates to wdf and mdf(replace the dates)
    wdf['Date'] = week_index
    mdf['Date'] = month_index

    # Update weekday since the dates are new
    weekdays(df,wdf,mdf)

    # Save to csv
    df.to_csv(f"{target}_df.csv", index=False)
    wdf.to_csv(f"{target}_wdf.csv", index=False)
    mdf.to_csv(f"{target}_mdf.csv", index=False)

    # Return a list of dataframes
    # These dataframes are not datetime indexed yet
    return [df,wdf,mdf] 


# In[16]:


dfs = [df,wdf,mdf] = data_prep(target,market)


# ## Calculating Tachnical Indicators

# ### BBAND

# In[19]:


@task
def bband(df,SMA,stdi):
    # Calculating bband daily
        bband_prep = pd.DataFrame()
        # ddof =0 : to have the same behavior as numpy.std ; this is the closet result to ileader, the trading app
        bband_prep['rolling_std'] = df['Close'].rolling(window = SMA).std(ddof=0)
        bband_prep['18MA'] = df['Close'].rolling(window = SMA).mean().round(2)
        bband_prep['+1 Band'] = (bband_prep['18MA'] + (bband_prep['rolling_std']*stdi)).round(2)
        bband_prep['-1 Band'] = (bband_prep['18MA'] - (bband_prep['rolling_std']*stdi)).round(2)

        # paste the result to the original df
        df['+1 Band'] = bband_prep['+1 Band']
        df['18MA'] = bband_prep['18MA']
        df['-1 Band'] = bband_prep['-1 Band']
    
        return bband_prep


# ### 50MA

# In[21]:


@task
def MA(df):
    df['50MA'] = df['Close'].rolling(window = 50).mean().round(2)


# ### KD Indicator

# In[23]:


# Create a function for KD indicators
# And return needed columns to original df
# RSV = (Close - Lowest Low)/(Highest High-Lowest Low)*100
# K = (2/3 * Previous K) +(1/3*RSV)
# D = (2/3 * Previous D) +(1/3*K)
@task
def kd_value(df,kd_window):
    
    # KDD
        # Create a dataframe fo storing kd values
        kd = df.copy()
        kd['Max price for past N periods'] = df['High'].rolling(kd_window).max()
        kd['Min price for past N periods'] = df['Low'].rolling(kd_window).min()

        # Calculate RSV value
        kd['RSV'] = (df['Close'] - kd['Min price for past N periods'])/(kd['Max price for past N periods']-kd['Min price for past N periods'])*100

        # The initial values has to be set at 1 row before RSV is calculated 
        # RSV is calculated at "kd_window" row ; ex. RSV appears at the 9th row(index 8)
        # Since index start from 0, the initial index of kd values = 7 
        kdd_initial_index = kd_window -2
        # Set initial KD values = 50
        kd.loc[kdd_initial_index,['K','D']] = 50

        # Set an index for starting calculation
        kd_cal_index = kd_window -1 

        # Should calculate kd values starts from kd_cal_index
        for i in range(0,len(kd)):
            if i >= kd_cal_index :
                kd.loc[i,'K'] = (kd.loc[i-1,'K']*(2/3)+ kd.loc[i,'RSV']*(1/3)).round(2)

        for i in range(0,len(kd)):
            if i >= kd_cal_index :
                kd.loc[i,'D'] = (kd.loc[i-1,'D']*(2/3)+ kd.loc[i,'K']*(1/3)).round(2)
                
        # Add kd values to the original df
        df['K'] = kd['K']
        df['D'] = kd['D']
        
        return kd


# ### Dynamic KD Indicator

# In[25]:


# n = week parameter 
# put in wdf(weekly data)
@task
def previous_period (agg_df,n):
    
# Prepare max/min price from past N-1 periods
    # Create a copy from wdf
    t1 = agg_df.copy()
    
    # Need to compare the highest high and lowest low of n-1 week to the current week
    # Calculate N-1 week's max and min (using weekly data) 
    t1['Max price from past N-1 periods'] = agg_df['High'].rolling(n-1).max()
    t1['Min price from past N-1 periods'] = agg_df['Low'].rolling(n-1).min()
    
    # Need to Paste max/min price from past N-1 periods to each day of the next week
    if 'week_key' in agg_df:
        # Grab next monday's date'Date'date
        t1['Next Monday'] = agg_df['Date'] + pd.offsets.Week(weekday=0)
        # Drop original 'Date' column 
        t1.drop('Date',axis=1,inplace=True)
        # Rename next monday as "Date" to match with daily data
        # To compare max/min price from past N-1 periods with accu max/min values next week
        t1 = t1.rename(columns={'Next Monday':'Date'})
      
    elif 'month_key' in agg_df:
        # Find the month_key for the next month
        t1['next 1st'] = agg_df['Date'] + pd.offsets.MonthBegin()
        # Drop original 'Date' column 
        t1.drop('Date',axis=1,inplace=True)
        # Rename next monday as "Date" to match with daily data
        # To compare max/min price from past N-1 periods with accu max/min values next week
        t1 = t1.rename(columns={'next 1st':'Date'})
     
    # Set Date as index
    t1.set_index('Date',inplace=True)
    
    # Resample weekly data to daily
    # Resample dataframe as Business days frequency(Use agg function : asfreq())
    t1 = t1.resample('B').asfreq()
    
    # Fill N-1 week values for each day this week(from Monday to Friday)
    # Forward fill (propagates the last valid observation forward)
    t1['Max price from past N-1 periods'].fillna(method='ffill',inplace=True)
    t1['Min price from past N-1 periods'].fillna(method='ffill',inplace=True)
    
    return t1[['Max price from past N-1 periods','Min price from past N-1 periods']]

@task
def accu_value(df):
    
    # Create a copy from df(daily data)
    t2 = df.copy()
    # Set datetime index 
    t2.set_index('Date',inplace=True)
    
    # Calculated accumulated High and Low(daily data from df) groupby week keys
    t2['acc_high_w'] = t2.groupby('week_key')['High'].cummax()
    t2['acc_low_w'] = t2.groupby('week_key')['Low'].cummin()

    t2['acc_high_m'] = t2.groupby('month_key')['High'].cummax()
    t2['acc_low_m'] = t2.groupby('month_key')['Low'].cummin()

    return t2[['acc_high_w','acc_low_w','acc_high_m','acc_low_m']]

@task
def dynamic_RSV(w1,m1,t2,df):
    
    # Combine past N-1 weeks data(w1) with accum data(t2)
    # Use the date on t2(original daily data from df): (how='left')
    # input t2 first to show t2 columns first
    # This table will show previous_period_rolling max and min + this period accumulated max and min
    merg_w = pd.merge(t2,w1,on='Date',how='left')
    merg_m = pd.merge(t2,m1,on='Date',how='left')

    # Drop unneccessary columns for each data frame
    merg_w.drop(["acc_high_m","acc_low_m"],axis=1,inplace=True)
    merg_m.drop(["acc_high_w","acc_low_w"],axis=1,inplace=True)
    
    # Add final max and min value for weekly data
    # Compare and select max and min btw N-1 periods highs and lows and accumulated highs and lows 
    merg_w['final_max_w'] = np.maximum(merg_w['acc_high_w'],merg_w['Max price from past N-1 periods'])
    merg_w['final_min_w'] = np.minimum(merg_w['acc_low_w'],merg_w['Min price from past N-1 periods'])

    # Add final max and min value for monthly data
    # Compare and select max and min btw N-1 periods highs and lows and accumulated highs and lows  
    merg_m['final_max_m'] = np.maximum(merg_m['acc_high_m'],merg_m['Max price from past N-1 periods'])
    merg_m['final_min_m'] = np.minimum(merg_m['acc_low_m'],merg_m['Min price from past N-1 periods'])

    # Adding Close price from df to weekly and monthly data
    daily = df.copy().set_index('Date')
    merg_w['Close']= daily['Close']
    merg_m['Close']= daily['Close']

    # RSV daily 
    
    merg_w['RSV(DW)'] = (merg_w['Close'] - merg_w['final_min_w'])/(merg_w['final_max_w']-merg_w['final_min_w'])*100
    merg_m['RSV(DM)'] = (merg_m['Close'] - merg_m['final_min_m'])/(merg_m['final_max_m']-merg_m['final_min_m'])*100

    return [merg_w,merg_m]

@task
def merge(df,wdf,mdf,merg_w,merg_m):

# Adding WKD and dynamic RSV to df

    # Select weekly data from wdf and resampled it with B frequency
    wd = wdf.copy().set_index('Date')
    md = mdf.copy().set_index('Date')
    
    # Resample wdf and mdf with Business days frequency
    # Select only weekly KD values from wdf
    wd = wd.resample('B').asfreq()
    wd = wd[['K','D']]
    md = md.resample('B').asfreq()
    md = md[['K','D']]
    
    # Combine df with wd/md, fit them on df's date
    merg_w2 = pd.merge(df.drop("month_key",axis=1),wd,on='Date',how='left')
    merg_m2 = pd.merge(df.drop("week_key",axis=1),md,on='Date',how='left')
    
    # Rename columns
    merg_w2 = merg_w2.rename(columns={'K_x':'K(D)','D_x':'D(D)','K_y':'K(W)','D_y':'D(W)'})
    merg_m2 = merg_m2.rename(columns={'K_x':'K(D)','D_x':'D(D)','K_y':'K(M)','D_y':'D(M)'})
    
    # Set index for merg2(need this step to add daily dynamic RSV)
    merg_w2.set_index('Date',inplace=True)
    merg_m2.set_index('Date',inplace=True)
    
    # Add daily dynamic RSV to merg2
    merg_w2['RSV(DW)'] = merg_w['RSV(DW)']
    merg_m2['RSV(DM)'] = merg_m['RSV(DM)']

    # WKD are formed and concluded on the last trading day of the week
    # Fillna with previous concluded WKD
    merg_w2['N-1 K(W)'] = merg_w2['K(W)']
    merg_w2['N-1 D(W)'] = merg_w2['D(W)']
    merg_w2['N-1 K(W)'].fillna(method='ffill',inplace=True)
    merg_w2['N-1 D(W)'].fillna(method='ffill',inplace=True)

    # MKD are formed and concluded on the last trading day of the month
    # Fillna with previous concluded WKD
    merg_m2['N-1 K(M)'] = merg_m2['K(M)']
    merg_m2['N-1 D(M)'] = merg_m2['D(M)']
    merg_m2['N-1 K(M)'].fillna(method='ffill',inplace=True)
    merg_m2['N-1 D(M)'].fillna(method='ffill',inplace=True)

    # Reset index for the next step 
    merg_w2.reset_index(inplace=True)
    merg_m2.reset_index(inplace=True)
    
    return[merg_w2,merg_m2]

# Calculating daily_WKD
# RSV = (Close - Lowest Low)/(Highest High-Lowest Low)*100
# K = (2/3 * Previous K) +(1/3*RSV)
# D = (2/3 * Previous D) +(1/3*K)
@task
def cal_DKD(df,agg_df,merg_agg_df):

    # df,wdf,mdf,merg_agg_df are now all not datetimeindexed
    
     # Setting up variables 
    if 'week_key' in merg_agg_df.columns: 
        RSV_Dy = 'RSV(DW)'
        previous_K_Dy =  'N-1 K(W)'
        previous_D_Dy =  'N-1 D(W)'
        K_Dy = 'K(DW)'
        D_Dy = 'D(DW)'
        K_agg ='K(W)'
        D_agg ='D(W)'
        merg2 = merg_agg_df
        
    elif 'month_key' in merg_agg_df.columns: 
        RSV_Dy = 'RSV(DM)'
        previous_K_Dy =  'N-1 K(M)'
        previous_D_Dy =  'N-1 D(M)'
        K_Dy = 'K(DM)'
        D_Dy = 'D(DM)'
        K_agg ='K(M)'
        D_agg ='D(M)'
        merg2 = merg_agg_df

    # Start calculation 1 row after the first valid RSV
    kd_cal_index = merg2[RSV_Dy].first_valid_index()+ 1

    for i in range(0,len(merg2)):
        
        # Start calculation if i >= kd_cal_index
        if i >= kd_cal_index :
            # If weekly K values equal to previous row's, calculate the new WK values
            if merg2.loc[i,previous_K_Dy] == merg2.loc[i-1,previous_K_Dy]:
                merg2.loc[i,K_Dy] = (merg2.loc[i,previous_K_Dy]*(2/3) + merg2.loc[i,RSV_Dy]*(1/3)).round(2)

            # Otherwise, WK doesn't change
            else :
                merg2.loc[i,K_Dy] = merg2.loc[i,previous_K_Dy]

    for i in range(0,len(merg2)):
        
        # Start calculation if i >= kd_cal_index
        if i >= kd_cal_index :
            # If weekly D values equal to previous row's, calculate the new WD values
            if merg2.loc[i,previous_D_Dy] == merg2.loc[i-1,previous_D_Dy]:
                merg2.loc[i,D_Dy] = (merg2.loc[i,previous_D_Dy]*(2/3) + merg2.loc[i,K_Dy]*(1/3)).round(2)

            # Otherwise, WK doesn't change
            else :
                merg2.loc[i,D_Dy] = merg2.loc[i,previous_D_Dy]

    # Drop unwanted columns
    merg2.drop([K_agg,D_agg,RSV_Dy,previous_K_Dy,previous_D_Dy],axis=1,inplace=True)
    merg2.set_index('Date',inplace=True)
    
    return merg2


# ### Subflow for Calculating Technical Indicators

# In[27]:


# target =stock tick, BB_SMA = 18, BB_stdi =1.2,MA_window = 50,kd_window = 9 
@flow
def technical(target,df,wdf,mdf,BB_SMA,BB_stdi,MA_window,kd_window):
    
# BBAND
    # Executing BBAND function to each dataframe
    dband = bband(df,BB_SMA,BB_stdi)
    wband = bband(wdf,BB_SMA,BB_stdi)
    mband = bband(mdf,BB_SMA,BB_stdi)

# 50MA for df
    df[f'{MA_window}MA'] = df['Close'].rolling(window = MA_window).mean().round(2)

# KD original
    # Execute kd function
    dkd = kd_value(df,kd_window)
    wkd = kd_value(wdf,kd_window)
    mkd = kd_value(mdf,kd_window)

    df.to_csv(f'{target}_df.csv',index=True)
    wdf.to_csv(f'{target}_wdf.csv',index=True)
    mdf.to_csv(f'{target}_mdf.csv',index=True)

    try : 
    # KD Dynamic 
        # 1. previous_period function
        w1 = previous_period(wdf,kd_window)
        m1 = previous_period(mdf,kd_window) 
        
        # 2. accu_value function
        t2 = accu_value(df) 
    
        # 3. dynamic_RSV function
        [merg_w,merg_m] = dynamic_RSV(w1,m1,t2,df)
    
        # 4. merge function
        [merg_w2,merg_m2]= merge(df,wdf,mdf,merg_w,merg_m)
    
        # 5. cal_DKD function
        dwkd = cal_DKD(df,wdf,merg_w2)
        dmkd = cal_DKD(df,mdf,merg_m2)
    
        # Combine two datasets
        dynamic_kd = pd.merge(dwkd[['K(DW)','D(DW)']],dmkd[['K(DM)','D(DM)']],on='Date',how='right')
    
        # Create a copy of df
        final_df = df.copy()
        
        # Set index to add dynamic kd values
        final_df.set_index('Date',inplace= True)
        final_df = final_df.rename(columns={'K':'K(D)','D':'D(D)'})
    
        # Add dynamic kd values to df
        final_df[['K(DW)','D(DW)','K(DM)','D(DM)']] = dynamic_kd[['K(DW)','D(DW)','K(DM)','D(DM)']]
        
        # Reset index
        final_df.reset_index(inplace=True)
        
        # Save to csv
     
        final_df.to_csv(f'{target}_df.csv',index=True)
        wdf.to_csv(f'{target}_wdf.csv',index=True)
        mdf.to_csv(f'{target}_mdf.csv',index=True)
           
        return [final_df,wdf,mdf]

    except Exception as e  :

        return [df,wdf,mdf]
        


# In[28]:


[df,wdf,mdf] = technical(target,df,wdf,mdf,18,1.2,50,9)


# ## Trading Signals

# ###  KD Trends(Increase/Decrease)

# In[31]:


@task
def kd_trend (df) :

     # Setting up variables 
    if 'week_key' in df.columns and 'K(W)' in df.columns: # wdf
         # Create shifted columns for comparison
        df['K(W)N-1'] = df['K(W)'].shift(1,axis=0)
        df['D(W)N-1'] = df['D(W)'].shift(1,axis=0)

        for i in range(0,len(df)):
            if np.isnan(df.loc[i,'K(W)']) == True or np.isnan(df.loc[i-1,'K(W)']) == True :
                df.loc[i,'K(W) trend'] = np.nan
                df.loc[i,'D(W) trend'] = np.nan
            else: 
                df.loc[i,'K(W) trend'] = df.loc[i,'K(W)']> df.loc[i,'K(W)N-1']
                df.loc[i,'D(W) trend'] = df.loc[i,'D(W)']> df.loc[i,'D(W)N-1']
    
        df['K(W) trend'] = df['K(W) trend'].map({True:'Increase',False:'Decrease'})
        df['D(W) trend'] = df['D(W) trend'].map({True:'Increase',False:'Decrease'})
      
        df.drop(['K(W)N-1','D(W)N-1'],axis=1,inplace=True)
        
    elif 'month_key' in df.columns and 'K(M)' in df.columns: # mdf
        # Create shifted columns for comparison
        df['K(M)N-1'] = df['K(M)'].shift(1,axis=0)
        df['D(M)N-1'] = df['D(M)'].shift(1,axis=0)

        for i in range(0,len(df)):
            if np.isnan(df.loc[i,'K(M)']) == True or np.isnan(df.loc[i-1,'K(M)']) == True :
                df.loc[i,'K(M) trend'] = np.nan
                df.loc[i,'D(M) trend'] = np.nan
            else: 
                df.loc[i,'K(M) trend'] = df.loc[i,'K(M)']> df.loc[i,'K(M)N-1']
                df.loc[i,'D(M) trend'] = df.loc[i,'D(M)']> df.loc[i,'D(M)N-1']
    
        df['K(M) trend'] = df['K(M) trend'].map({True:'Increase',False:'Decrease'})
        df['D(M) trend'] = df['D(M) trend'].map({True:'Increase',False:'Decrease'})
        
        df.drop(['K(M)N-1','D(M)N-1'],axis=1,inplace=True)
        
    else : # df             
        # Create shifted columns for comparison
        df['K(D)N-1'] = df['K(D)'].shift(1,axis=0)
        df['D(D)N-1'] = df['D(D)'].shift(1,axis=0)
        df['K(DW)N-1'] = df['K(DW)'].shift(1,axis=0)
        df['D(DW)N-1'] = df['D(DW)'].shift(1,axis=0)
        df['K(DM)N-1'] = df['K(DM)'].shift(1,axis=0)
        df['D(DM)N-1'] = df['D(DM)'].shift(1,axis=0)
      
        for i in range(0,len(df)):
            if np.isnan(df.loc[i,'K(D)']) == True or np.isnan(df.loc[i-1,'K(D)']) == True :
                df.loc[i,'K(D) trend'] = np.nan
                df.loc[i,'D(D) trend'] = np.nan
            else: 
                df.loc[i,'K(D) trend'] = df.loc[i,'K(D)']> df.loc[i,'K(D)N-1']
                df.loc[i,'D(D) trend'] = df.loc[i,'D(D)']> df.loc[i,'D(D)N-1']
    
        df['K(D) trend'] = df['K(D) trend'].map({True:'Increase',False:'Decrease'})
        df['D(D) trend'] = df['D(D) trend'].map({True:'Increase',False:'Decrease'})
      
        for i in range(0,len(df)):
            if np.isnan(df.loc[i,'K(DW)']) == True or np.isnan(df.loc[i-1,'K(DW)']) == True :
                df.loc[i,'K(DW) trend'] = np.nan
                df.loc[i,'D(DW) trend'] = np.nan
            else: 
                df.loc[i,'K(DW) trend'] = df.loc[i,'K(DW)']> df.loc[i,'K(DW)N-1']
                df.loc[i,'D(DW) trend'] = df.loc[i,'D(DW)']> df.loc[i,'D(DW)N-1']
    
        df['K(DW) trend'] = df['K(DW) trend'].map({True:'Increase',False:'Decrease'})
        df['D(DW) trend'] = df['D(DW) trend'].map({True:'Increase',False:'Decrease'})
    
        for i in range(0,len(df)):
            if np.isnan(df.loc[i,'K(DM)']) == True or np.isnan(df.loc[i-1,'K(DM)']) == True :
                df.loc[i,'K(DM) trend'] = np.nan
                df.loc[i,'D(DM) trend'] = np.nan
            else: 
                df.loc[i,'K(DM) trend'] = df.loc[i,'K(DM)']> df.loc[i,'K(DM)N-1']
                df.loc[i,'D(DM) trend'] = df.loc[i,'D(DM)']> df.loc[i,'D(DM)N-1']
    
        df['K(DM) trend'] = df['K(DM) trend'].map({True:'Increase',False:'Decrease'})
        df['D(DM) trend'] = df['D(DM) trend'].map({True:'Increase',False:'Decrease'})
        
        df.drop(['K(D)N-1','D(D)N-1','K(DW)N-1','D(DW)N-1','K(DM)N-1','D(DM)N-1'],axis=1,inplace=True)
    
    return df


# ### K > D ?

# In[33]:


@task
def kd_comparison(df):
    
    if 'week_key' in df.columns and 'K(W)' in df.columns: # wdf
        for i in range(0,len(df)):
            if np.isnan(df.loc[i,'K(W)']) == True :
                df.loc[i,'K(W)>D(W)'] = np.nan
            else: 
                df.loc[i,'K(W)>D(W)'] = df.loc[i,'K(W)'] > df.loc[i,'D(W)']

        df['K(W)>D(W)'] = df['K(W)>D(W)'].map({True:'Yes',False:'No'})
                
    elif 'month_key' in df.columns and 'K(M)' in df.columns: # mdf
        for i in range(0,len(df)):
            if np.isnan(df.loc[i,'K(M)']) == True :
                df.loc[i,'K(M)>D(M)'] = np.nan
            else: 
                df.loc[i,'K(M)>D(M)'] = df.loc[i,'K(M)'] > df.loc[i,'D(M)']

        df['K(M)>D(M)'] = df['K(M)>D(M)'].map({True:'Yes',False:'No'})

    else : #df  
        for i in range(0,len(df)):
            if np.isnan(df.loc[i,'K(D)']) == True :
                df.loc[i,'K(D)>D(D)'] = np.nan
            else: 
                df.loc[i,'K(D)>D(D)'] = df.loc[i,'K(D)'] > df.loc[i,'D(D)']
           
        df['K(D)>D(D)'] = df['K(D)>D(D)'].map({True:'Yes',False:'No'})
    
        for i in range(0,len(df)):
            if np.isnan(df.loc[i,'K(DW)']) == True :
                df.loc[i,'K(DW)>D(DW)'] = np.nan
            else: 
                df.loc[i,'K(DW)>D(DW)'] = df.loc[i,'K(DW)'] > df.loc[i,'D(DW)']
           
        df['K(DW)>D(DW)'] = df['K(DW)>D(DW)'].map({True:'Yes',False:'No'})
    
        for i in range(0,len(df)):
            if np.isnan(df.loc[i,'K(DM)']) == True :
                df.loc[i,'K(DM)>D(DM)'] = np.nan
            else: 
                df.loc[i,'K(DM)>D(DM)'] = df.loc[i,'K(DM)'] > df.loc[i,'D(DM)']
           
        df['K(DM)>D(DM)'] = df['K(DM)>D(DM)'].map({True:'Yes',False:'No'})

    return df


# ### KD Golden Cross and Death Cross

# In[35]:


@task
# Golden cross/death cross
# Golden cross = previous K<=D, current K>D
# Death cross = previous K>=D, current K<D
def kd_cross(df):
    
    if 'week_key' in df.columns and 'K(W)' in df.columns: # wdf
        for i in range(0,len(df)):
            if np.isnan(df.loc[i,'K(W)']) == True or np.isnan(df.loc[i-1,'K(W)']) == True:
                df.loc[i,'kd_cross'] = np.nan
    
            else: 
                if (df.loc[i-1,'K(W)'] <= df.loc[i-1,'D(W)']) & (df.loc[i,'K(W)'] > df.loc[i,'D(W)']):
                     df.loc[i,'kd_cross'] = 'Golden Cross'
                elif (df.loc[i-1,'K(W)'] >= df.loc[i-1,'D(W)']) & (df.loc[i,'K(W)'] < df.loc[i,'D(W)']):
                     df.loc[i,'kd_cross'] = 'Death Cross'
                else : 
                    df.loc[i,'kd_cross']='None'

    elif 'month_key' in df.columns and 'K(M)' in df.columns: # mdf  
        for i in range(0,len(df)):
            if np.isnan(df.loc[i,'K(M)']) == True or np.isnan(df.loc[i-1,'K(M)']) == True:
                df.loc[i,'kd_cross'] = np.nan
    
            else: 
                if (df.loc[i-1,'K(M)'] <= df.loc[i-1,'D(M)']) & (df.loc[i,'K(M)'] > df.loc[i,'D(M)']):
                     df.loc[i,'kd_cross'] = 'Golden Cross'
                elif (df.loc[i-1,'K(M)'] >= df.loc[i-1,'D(M)']) & (df.loc[i,'K(M)'] < df.loc[i,'D(M)']):
                     df.loc[i,'kd_cross'] = 'Death Cross'
                else : 
                    df.loc[i,'kd_cross']='None'

    else : #df 
        for i in range(0,len(df)):
            if np.isnan(df.loc[i,'K(D)']) == True or np.isnan(df.loc[i-1,'K(D)']) == True:
                df.loc[i,'kdd_cross'] = np.nan
    
            else: 
                if (df.loc[i-1,'K(D)'] <= df.loc[i-1,'D(D)']) & (df.loc[i,'K(D)'] > df.loc[i,'D(D)']):
                     df.loc[i,'kdd_cross'] = 'Golden Cross'
                elif (df.loc[i-1,'K(D)'] >= df.loc[i-1,'D(D)']) & (df.loc[i,'K(D)'] < df.loc[i,'D(D)']):
                     df.loc[i,'kdd_cross'] = 'Death Cross'
                else : 
                    df.loc[i,'kdd_cross']='None'
    
        for i in range(0,len(df)):
            if np.isnan(df.loc[i,'K(DW)']) == True or np.isnan(df.loc[i-1,'K(D)']) == True:
                df.loc[i,'kdw_cross'] = np.nan
    
            else: 
                if (df.loc[i-1,'K(DW)'] <= df.loc[i-1,'D(DW)']) & (df.loc[i,'K(DW)'] > df.loc[i,'D(DW)']):
                     df.loc[i,'kdw_cross'] = 'Golden Cross'
                elif (df.loc[i-1,'K(DW)'] >= df.loc[i-1,'D(DW)']) & (df.loc[i,'K(DW)'] < df.loc[i,'D(DW)']):
                     df.loc[i,'kdw_cross'] = 'Death Cross'
                else : 
                    df.loc[i,'kdw_cross']='None'
                    
        for i in range(0,len(df)):
            if np.isnan(df.loc[i,'K(DM)']) == True or np.isnan(df.loc[i-1,'K(D)']) == True:
                df.loc[i,'kdm_cross'] = np.nan
    
            else: 
                if (df.loc[i-1,'K(DM)'] <= df.loc[i-1,'D(DM)']) & (df.loc[i,'K(DM)'] > df.loc[i,'D(DM)']):
                     df.loc[i,'kdm_cross'] = 'Golden Cross'
                elif (df.loc[i-1,'K(DM)'] >= df.loc[i-1,'D(DM)']) & (df.loc[i,'K(DM)'] < df.loc[i,'D(DM)']):
                     df.loc[i,'kdm_cross'] = 'Death Cross'
                else : 
                    df.loc[i,'kdm_cross']='None'
      
    return df


# ### Candlestick patterns

# In[37]:


# Open > Close = Bearish, Close > Open = Bullish 
@task
def candlestick_pat(df):
    for i in range(0,len(df)):
        if df.loc[i,'Open']> df.loc[i,'Close'] :
            df.loc[i,'candle_stick'] = 'Bearish'
        elif df.loc[i,'Open']< df.loc[i,'Close'] :
            df.loc[i,'candle_stick'] = 'Bullish'
        else :
            df.loc[i,'candle_stick'] = 'Flat'
        
    return df


# ### BBAND Status

# In[39]:


@task
def bband_stat(df):
    for i in range(0,len(df)):
        # +1 Area
        if (df.loc[i,'Open']> df.loc[i,'+1 Band']) & (df.loc[i,'Close']> df.loc[i,'+1 Band']):
            df.loc[i,'bband_stat'] = 'Strong increase trend' 
            
        #突破 +1
        elif (df.loc[i,'Open']<= df.loc[i,'+1 Band']) & (df.loc[i,'Close']> df.loc[i,'+1 Band']):
            df.loc[i,'bband_stat'] = 'Surpass +1 Band' 
        #跌破 +1
        elif (df.loc[i,'Open']> df.loc[i,'+1 Band']) & (df.loc[i,'Close']<= df.loc[i,'+1 Band']):
            df.loc[i,'bband_stat'] = 'Fall below +1 Band' 
    
        # +1 band ~ 18MA 
        elif (df.loc[i,'18MA']< df.loc[i,'Open']< df.loc[i,'+1 Band']) & (df.loc[i,'18MA']< df.loc[i,'Close']< df.loc[i,'+1 Band']):
            df.loc[i,'bband_stat'] = '18MA ~ +1 Band' 
        
        #突破 18MA
        elif (df.loc[i,'-1 Band']< df.loc[i,'Open']< df.loc[i,'18MA']) & (df.loc[i,'18MA']< df.loc[i,'Close']< df.loc[i,'+1 Band']):
            df.loc[i,'bband_stat'] = 'Surpass 18 MA' 
        
        #跌破 18MA
        elif (df.loc[i,'18MA'] < df.loc[i,'Open']< df.loc[i,'+1 Band']) & (df.loc[i,'-1 Band']< df.loc[i,'Close']< df.loc[i,'18MA']):
            df.loc[i,'bband_stat'] = 'Fall below 18MA' 
            
        # -1 band ~ 18MA   
        elif (df.loc[i,'-1 Band']< df.loc[i,'Open']< df.loc[i,'18MA']) & (df.loc[i,'-1 Band']< df.loc[i,'Close']< df.loc[i,'18MA']):
            df.loc[i,'bband_stat'] = '-1 Band ~ 18 MA'   

        #突破-1 
        elif (df.loc[i,'Open']< df.loc[i,'-1 Band']) & (df.loc[i,'Close']>= df.loc[i,'-1 Band']):
            df.loc[i,'bband_stat'] = 'Surpass -1 Band'       
        #跌破 -1
        elif (df.loc[i,'Open']>= df.loc[i,'-1 Band']) & (df.loc[i,'Close']< df.loc[i,'-1 Band']):
            df.loc[i,'bband_stat'] = 'Fall below -1 Band' 
        # -1 Area
        elif (df.loc[i,'Open']< df.loc[i,'-1 Band']) & (df.loc[i,'Close']< df.loc[i,'-1 Band']):
            df.loc[i,'bband_stat'] = 'Strong decrease trend' 
               
    return df


# ### 18 V.S 50 MA

# In[41]:


@task
def MA_trend (df):

#18MA, 50MA trend
    # 18MA
    for i in range(0,len(df)):
        if np.isnan(df.loc[i,'18MA']) == True or np.isnan(df.loc[i-1,'18MA']) == True:
            df.loc[i,'18MA trend'] = np.nan
        else :
            df.loc[i,'18MA trend'] = df.loc[i,'18MA']> df.loc[i-1,'18MA']
    # 50 MA
    for i in range(0,len(df)):
        if np.isnan(df.loc[i,'50MA']) == True or np.isnan(df.loc[i-1,'50MA']) == True:
            df.loc[i,'50MA trend'] = np.nan
        else :
            df.loc[i,'50MA trend'] = df.loc[i,'50MA']> df.loc[i-1,'50MA']
    
    df['18MA trend'] = df['18MA trend'].map({True:'Increase',False:'Decrease'})
    df['50MA trend'] = df['50MA trend'].map({True:'Increase',False:'Decrease'})

# 18MA and 50 MA golden cross/death cross
    for i in range(0,len(df)):
        if np.isnan(df.loc[i,'18MA']) == True or np.isnan(df.loc[i,'50MA']) == True:
            df.loc[i,'18MA_50MA'] = np.nan
        elif (df.loc[i-1,'18MA'] - df.loc[i-1,'50MA']<=0) & (df.loc[i,'18MA'] - df.loc[i,'50MA']>0)  :
            df.loc[i,'18MA_50MA'] = 'Golden Cross'
        elif df.loc[i,'18MA'] - df.loc[i,'50MA']>0 :
            df.loc[i,'18MA_50MA'] = 'Positive'
        elif (df.loc[i-1,'18MA'] - df.loc[i-1,'50MA']>=0) & (df.loc[i,'18MA'] - df.loc[i,'50MA']<0)  :
            df.loc[i,'18MA_50MA'] = 'Death Cross'
        elif df.loc[i,'18MA'] - df.loc[i,'50MA']<0 :
            df.loc[i,'18MA_50MA'] = 'Negative'

# Close price above 50 MA?
    for i in range(0,len(df)):
        if np.isnan(df.loc[i,'50MA']) == True:
            df.loc[i,'Close_50MA'] = np.nan
        elif df.loc[i,'Close']> df.loc[i,'50MA']:
            df.loc[i,'Close_50MA'] = 'Above 50MA'
        elif df.loc[i,'Close']== df.loc[i,'50MA']:
            df.loc[i,'Close_50MA'] = 'Equal to 50MA'
        else :
            df.loc[i,'Close_50MA'] = 'Below 50MA'
        
    return df


# ### Subflow for Trading Signals

# In[43]:


@flow
def trading_signal(target,df,wdf,mdf):
    try : 
        # Rename KD columns
        wdf.rename(columns={'K':'K(W)','D':'D(W)'},inplace=True) 
        mdf.rename(columns={'K':'K(M)','D':'D(M)'},inplace=True) 
        # functions
        df = kd_trend(df)
        wdf = kd_trend(wdf)
        mdf = kd_trend(mdf)
        df = kd_comparison(df)
        wdf = kd_comparison(wdf)
        mdf = kd_comparison(mdf)
        df = kd_cross(df)
        wdf = kd_cross(wdf)
        mdf = kd_cross(mdf)
        df = candlestick_pat(df)
        wdf = candlestick_pat(wdf)
        mdf = candlestick_pat(mdf)
        df = bband_stat(df)
        wdf = bband_stat(wdf)
        mdf = bband_stat(mdf)
        df = MA_trend(df)
    
        #Save and update df
        df.to_csv(f'{target}_df.csv',index=True)
        wdf.to_csv(f'{target}_wdf.csv',index=True)
        mdf.to_csv(f'{target}_mdf.csv',index=True)
        
        return [df,wdf,mdf]

    except Exception as e :
        return [df,wdf,mdf]


# In[44]:


[df,wdf,mdf] = trading_signal(target,df,wdf,mdf)


# In[45]:


df.tail()


# In[46]:


wdf.tail()


# In[47]:


mdf.tail()


# ## Other Tasks

# ### Download Updated Stock List

# In[50]:


@task
def stocklist_download():
    # Download Taiwan stock lists from TWSE website.
    # Returns lists of stock symbols for public, OTC, and emerging stocks.
    # 1:public stocks,2:OTC stocks, 3:emerging stocks
   
    url_1 = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
    url_2 = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=4'
    url_3 = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=5'

    try:
        # 1st 0 = to get the table, 2nd 0 =to get the first column that contains stock symbol
        public = pd.read_html(url_1)[0][0]
        OTC = pd.read_html(url_2)[0][0]
        emerging = pd.read_html(url_3)[0][0]

        # Extract only first 4 numbers for each row-- these are stock symbols
        # Extract starts from index 2 to 1036 (publicly listed company)
        # Convert data type to integer
        public = public.iloc[2:1037].str[:4].astype(int)    
        OTC = OTC.iloc[10738:11617].str[:4].astype(int)   
        emerging = emerging.iloc[1:].str[:4].astype(int) 
    
        # Make series to lists of strings
        public_stocklist = [str(num) for num in list(public)]
        OTC_stocklist = [str(num) for num in list(OTC)]
        emerging_stocklist = [str(num) for num in list(emerging)]
    
        return [public_stocklist,OTC_stocklist,emerging_stocklist]
        
    except Exception as e:
        print(f"Error downloading :{str(e)}")
           


# In[51]:


# [public_stocklist,OTC_stocklist,emerging_stocklist] = stocklist_download()


# In[52]:


# public_stocklist[660:665]


# In[53]:


stocklist = ['4438','4439','4440','4441','4526']


# ### Createing Database in PgAdmin

# In[55]:


from sqlalchemy import create_engine
from prefect import get_run_logger

@task
#Create connection
def create_database_engine():
    
    # Database configuration (store these in environment variables or Prefect Blocks)
    DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'Stock',
    'user': 'postgres',
    'password': '790511'} 
    
    # Create Prefect logger instance 
    logger = get_run_logger()
    
    # Create SQLAlchemy engine for PostgreSQL
    # Connection string for PostgreSQL
    conn_string = (f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

    # Add connection pooling and other optimizations
    engine = create_engine(
        conn_string,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,  # Verify connections before using
        echo=False  # Set to True for SQL logging
    )

     # Test connection
    try:
        with engine.connect() as conn:
            logger.info(f"✅ Successfully connected to PostgreSQL database: {DB_CONFIG['database']}")
            
        return engine
        
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise


# In[56]:


engine = create_database_engine()


# ### Auto Save to PgAdmin

# In[58]:


import psycopg2
from prefect import get_run_logger
from prefect.cache_policies import NO_CACHE

@task(cache_policy=NO_CACHE)
def save_to_database(target,engine,df,wdf,mdf):

    logger = get_run_logger()

    try:
        # Auto-import tables from CSV files
        # index = True; show DatetimeIndex
        # Use 'replace' to drop/create, 'append' to add to existing table
        df.to_sql(f"{target}_df", engine, if_exists='replace', index=False)
        print(f"Successfully imported {len(df)} rows to table df ")
            
        # index = True; show DatetimeIndex
        wdf.to_sql(f"{target}_wdf", engine, if_exists='replace', index=False)
        print(f"Successfully imported {len(wdf)} rows to table wdf ")
             
        # index = True; show DatetimeIndex
        mdf.to_sql(f"{target}_mdf", engine, if_exists='replace', index=False)
        print(f"Successfully imported {len(mdf)} rows to table mdf ")

    except Exception as e :
        logger.error(f"Error saving to database: {str(e)}")
        raise  # Re-raise to mark task as failed


# In[59]:


save_to_database(target,engine,df,wdf,mdf)


# ### Main Flow & Deployment 

# A cron expression defines when the flow should run. There are 5 * in cron. If you set a * as *, means you want to run every unit. For example, set the third * as * will mean the deployment runs everyday. Remember to leave proper spacing between each cron field. 
# 
# 1. minute (0 - 59)
# 2. hour (0 - 23)
# 3. day of month (1 - 31)
# 4. month (1 - 12)
# 5. day of week (0 - 6) (Sunday=0 or 7)

# In[ ]:


import time
import nest_asyncio
nest_asyncio.apply()
import asyncio
from prefect.schedules import Cron

@flow(log_prints=True)
async def stock_transform():
    start_time =time.perf_counter()
    # Create an engine
    engine = create_database_engine()
    # Download stock lists
    stock_list = stocklist
    market = 'TW'
    
    for i in stock_list: 
        try:
            [df,wdf,mdf] = data_prep(i,market)
            print(f"Data {i} downloaded successfully")
            
            try:
                [df,wdf,mdf] = technical(i,df,wdf,mdf,18,1.2,50,9)
                print(f"Data {i} calculated successfully")

                try:
                    [df,wdf,mdf] = trading_signal(i,df,wdf,mdf)
                    print(f"Data {i} signals added successfully")
                    save_to_database(i,engine,df,wdf,mdf)
                    print(f"Data {i} table create in database successfully")

                except Exception as e :
                    print(f"Data {i} signals added failed")
                    print(f"Error : {e}")

            except Exception as e :
                print(f"Data {i} calculated failed")
                print(f"Error : {e}")
            
        except Exception as e :
            print(f"Data {i} downloaded failed")
            print(f"Error : {e}")
        

                    
    end_time = time.perf_counter()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.4f} seconds")
    
if __name__ == "__main__" :
    # Only for local testing/running
    asyncio.run(stock_transform())
    
    # Create a deployment with .serve 
    # cron="00 14 * * *", # minute, hour, day of month, month, day of week
    stock_transform.source(
        source = "",
        entrypoint = "deploy.py:stock_transform"
    
    ).deploy(
        name="stock-deployment",
        work_pool_name = "my-work-pool",
        schedule = Cron(
            # "0 14 * * *", # Runs at 14:00 at Taipei,Taiwan time 
            # timezone = "Asia/Taipei"
            "00 16 * * *", # Runs at EST time
            timezone = "America/New_York"
        )
    )


# In[ ]:


# # Run it once before deployment 
# from test import stock_transform # 

# if __name__ == "__main__":
# # Deployment
#     stock_transform.from_source(
#         # current directory
#         source=".",
#         entrypoint = "test.py:stock_transform"
#     ).deploy(
#         name="stock-deployment",
#         work_pool_name = "my-work-pool"
#     )

