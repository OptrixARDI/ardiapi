import requests
import json
import pandas as pd
import argparse
import datetime
from dateutil import tz
import os
import matplotlib
import matplotlib.pyplot as plt
#import templateblock

global commonsettings
commonsettings = {}
commonsettings['supportcount'] = 149
commonsettings['mgoffset'] = 10
commonsettings['tgoffset'] = 10

def query(server,qry):
    #print("Requesting..." + "http://" + server + "/api/aql/query?query=" + qry)
    req = requests.post("http://" + server + "/api/aql/query",{ "query": qry })    
    content = req.text
    #print(content)
    return json.loads(content)

def cvFloat(dta):
    if dta == "^":
        return None
    else:
        return float(dta)

def cvInt(dta):
    if dta == "^":
        return None
    else:
        return int(dta)

def ConvertTZ(dt, fromtz, totz):    
    local = datetime.datetime.strptime(dt,'%Y-%m-%d %H:%M:%S')
    local = local.replace(tzinfo=fromtz)
    return local.astimezone(totz).strftime('%Y-%m-%d %H:%M:%S')

def TrimDataFrame(df,start,end):
    try:        
        stpnt = df.index.get_loc(start,method='pad')
        enpnt = df.index.get_loc(end,method='backfill')
    except:
        df = df[~df.index.duplicated(keep='first')]
        try:
            stpnt = df.index.get_loc(start,method='pad')
            enpnt = df.index.get_loc(end,method='backfill')
        except:
            return df
    dfx = df[start:end]

    pre = None
    try:
        pre = df[:start].fillna(method='ffill')
    except:
        pass
    
    print("Closest Start Index = " + str(stpnt) + " = " + str(df.index[stpnt]) + ' vs ' + str(start))
    if df.index[stpnt] < start:
        print("Appending Start Value...")
        if pre is not None and len(pre) > 0:
            newdata = [pre.iloc[len(pre)-1].values]
        else:
            newdata = [df.iloc[stpnt].values]
        newindex = [start]
        tdf = pd.DataFrame(data=newdata,columns=dfx.columns,index=newindex)
        dfx = pd.concat([tdf,dfx])
    else:
        if pre is not None and len(pre) > 0:
            print("Filling In Start Values...")
            for n in pre.columns:            
                dfx.iloc[0][n] = pre.iloc[len(pre)-1][n]            
            pass
    
    
    #print("Closest End Index = " + str(enpnt) + " = " + str(df.index[enpnt]) + ' vs ' + str(end))
    if df.index[enpnt] > end:
        print("Padding End Value...")
        newdata = [df.iloc[enpnt].values]
        newindex = [end]
        tdf = pd.DataFrame(data=newdata,columns=dfx.columns,index=newindex)
        dfx = pd.concat([dfx,tdf])
        #print(str(dfx))
    
    return dfx    

def ReportArgs(name):
    deftz = "Australia/Sydney"
    defserver = "localhost/s/default"
    defname = "ARDI Server"
    defcode = "ARDI"
    
    #Load Defaults
    try:
        
        basepath = os.path.dirname(os.path.abspath(__file__))
        print(basepath)

        fl = open(basepath + "/settings.txt", "r")
        lines = fl.readlines()
        defserver = lines[3].strip()
        defname = lines[0].strip()
        defcode = lines[1].strip()
        deftz = lines[2].strip()
        fl.close()
    except:
        pass

    #Get command-line parameters
    parser = argparse.ArgumentParser(description="Create " + name)
    parser.add_argument('startdate',help='The start date for the report')
    parser.add_argument('enddate', help='The end date for the report')
    parser.add_argument('target', help='The file/folder to write output to')
    parser.add_argument('timezone',help='The timezone to use',default=deftz)
    parser.add_argument('--nopng',dest='nopng',action='store_const',const=True,default=False)
    parser.add_argument('--server',dest='server',default=defserver)
    parser.add_argument('--param',dest='param',default=None)
    args = parser.parse_args()

    args.local_zone = tz.gettz(args.timezone)
    args.server_zone = tz.gettz('UTC')    

    localst = datetime.datetime.strptime(args.startdate,'%Y-%m-%d %H:%M:%S')
    localst = localst.replace(tzinfo=args.local_zone)
    utcst = localst.astimezone(args.server_zone)

    localen = datetime.datetime.strptime(args.enddate,'%Y-%m-%d %H:%M:%S')
    localen = localen.replace(tzinfo=args.local_zone)
    utcen = localen.astimezone(args.server_zone)

    args.location = defname
    args.code = defcode

    if utcst > utcen:
        a = utcen
        utcen = utcst
        utcst = a

        a = localst
        localst = localen
        localen = a

    args.localstart = localst
    args.localend = localen
    
    args.start = utcst.strftime('%Y-%m-%d %H:%M:%S')
    args.end = utcen.strftime('%Y-%m-%d %H:%M:%S')

    args.utcstart = utcst
    args.utcend = utcen

    global commonsettings
    args.common = commonsettings

    return args


    
def frameToSequence(frame):
    x = len(frame)
    #print(str(frame))
    newindex = range(0,x,1)
    df = frame.reset_index()
    df = df.drop('index',axis=1)
    return df

def pointlistToDataFrame(results):
    columns = []
    for q in results['results']:        
        if q['type'] == "pointlist":
            for r in q['value']:
                columns.append(r['name'] + " " + r['propname'])
    return pd.DataFrame(columns=columns)
    
def historyToDataFrame(results,namemap=None,report=None,mapna=None):
    indx = -1
    frames = []
    
    for q in results['results']:        
        if q['type'] == "pointlist":
            #print(str(q['value']))
            for r in q['value']:                
                indx = indx + 1
                timeseries = None
                try:
                    timeseries = r['history']
                except:
                    pass

                if timeseries is None:
                    continue
                
                sname = r['name'] + " " + r['propname']
                if namemap is not None:
                    try:
                        sname = namemap[indx]
                    except:
                        pass

                if report == None:
                    dindex = pd.DatetimeIndex([i[0] for i in timeseries])
                else:
                    dindex = pd.DatetimeIndex([ConvertTZ(i[0],report.server_zone,report.local_zone) for i in timeseries])
                try:
                    if len(r['map']) > 0:
                        pass
                    frames.append(pd.DataFrame([cvInt(i[1]) for i in timeseries],columns=[sname],index=dindex))                   
                except:
                    frames.append(pd.DataFrame([cvFloat(i[1]) for i in timeseries],columns=[sname],index=dindex))                    
    #print(frames)
    #print("Finishing...")

    final = None
    for n in frames:
        if mapna is not None:
            for x in mapna:
                if x[0] in n.columns:
                    if x[1] == 'hold':
                        n = n.fillna(method='bfill')
                        n = n.fillna(method='ffill')
                    else:
                        n = n.fillna(value=x[1])                        
                        
        if final is None:
            final = n
        else:
            #print(str(n))
            final = final.join(n,how='outer')

    if final is None:        
        return pointlistToDataFrame(results)
    
    return final
