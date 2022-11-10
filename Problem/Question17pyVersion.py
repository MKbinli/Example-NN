import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import tensorflow as tf
from datetime import datetime, timedelta

raw_dataset=pd.read_csv("municipality_bus_utilization.csv", na_values = "?")
dataset= raw_dataset.copy()
print(dataset.isna().sum())

tm=dataset["timestamp"].copy()
mk=pd.to_datetime(pd.to_datetime(tm).dt.strftime(
    '%Y-%m-%d %H:00:00')).map(pd.Timestamp.timestamp).astype(int)
#Let's delete fist missing data refer to 7:00 AM and reset index values
firstMissingDataTime=dataset["timestampSeconds"].at[0]
dataset=dataset[dataset.timestampSeconds!=firstMissingDataTime]

mk2=pd.to_datetime(mk,unit="s")#Reverse

def anyMissingRows(df):
    checkDf=df.copy()
    firstTimestamp=checkDf.at[0,"timestampSeconds"]
    lastTimestamp=checkDf.at[len(checkDf)-1,"timestampSeconds"]
    timeDifference=3600#1 hour equals to 3600 seconds
    timeDifferenceOneDay=3600*24
    lastTimestamp=lastTimestamp+timeDifference
    for timeStmp in range(firstTimestamp,lastTimestamp,timeDifference):
        numberOfBus=len(checkDf[checkDf.timestampSeconds==timeStmp])
        if(numberOfBus==0):
            #Value is missing in this specific time. We apply that filling missing value with previous but 24 hours before
            fillingData=checkDf[checkDf.timestampSeconds == timeStmp-timeDifferenceOneDay].copy()
            if(len(fillingData)<= 0):#If 1 day before not exists
                fillingData=checkDf[checkDf.timestampSeconds == timeStmp-timeDifference].copy()
            fillingData["timestampSeconds"]=timeStmp
            fillingData["timestamp"]=pd.to_datetime(fillingData["timestampSeconds"],unit="s")
            checkDf=pd.concat([checkDf,fillingData],axis=0)
            checkDf.reset_index(drop=True, inplace=True)
        
    checkDf=checkDf.sort_values(by=["timestampSeconds","municipality_id"])

    return checkDf

def anyMissingBus(df):
    checkDf=df.copy()
    firstTimestamp=checkDf.at[0,"timestampSeconds"]
    lastTimestamp=checkDf.at[len(checkDf)-1,"timestampSeconds"]
    timeDifference=3600#1 hour equals to 3600 seconds
    lastTimestamp=lastTimestamp+timeDifference
    busNumber=[]
    for timeStmp in range(firstTimestamp,lastTimestamp,timeDifference):
        numberOfBus=len(checkDf[checkDf.timestampSeconds==timeStmp])
        if(numberOfBus!=10):#numberOfBus<=18 and 
            #Value is missing in this specific time. We apply that filling missing value with previous but 24 hours before
            busNumber.append(timeStmp)
        
    #checkDf=checkDf.sort_values(by=["timestampSeconds","municipality_id"])

    return busNumber

def busUsageMax(df):
    checkDf=df.copy()
    busUsageMaxDf=pd.DataFrame(columns=df.columns)
    firstTimestamp=checkDf.at[0,"timestampSeconds"]
    lastTimestamp=checkDf.at[len(checkDf)-1,"timestampSeconds"]
    timeDifference=3600#1 hour equals to 3600 seconds
    lastTimestamp=lastTimestamp+timeDifference
    busNumbers=list(range(0,10))
    for timeStmp in range(firstTimestamp,lastTimestamp,timeDifference):
        tempDf=checkDf[checkDf.timestampSeconds==timeStmp].copy()
        for bus in busNumbers:
            newdf=tempDf[tempDf.municipality_id==bus]
            newdf=newdf[newdf.usage==tempDf.loc[tempDf.municipality_id==bus,"usage"].max()]
            if(len(newdf)>1):#More than one values can be same
                busUsageMaxDf=busUsageMaxDf.append(newdf.iloc[0])
            else:
                busUsageMaxDf=busUsageMaxDf.append(newdf)
        
    busUsageMaxDf=busUsageMaxDf.sort_values(by=["timestampSeconds","municipality_id"])
    busUsageMaxDf.reset_index(drop=True, inplace=True)

    return busUsageMaxDf

nmk=pd.concat([mk,fillingData],axis=0)
dataset.loc[dataset.usage > dataset.total_capacity,"usage"]=dataset.loc[dataset.usage > dataset.total_capacity,"total_capacity"]
