import requests
import xmltodict
import pandas as pd
import numpy as np
import json
import traceback
import time
from dateutil import tz
import datetime
import pytz

try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode

#This defines a single ARDI context - a port to READ from and one to WRITE to
class Context:
    def __init__(self):
        self.consolidator = 5336
        self.submission = 2991
        self.name = "Actual"
        self.server = ""

#Represents a single ARDI server
class Server:
    def __init__(self, srv, site='default', port=None, secure=True):
        #The full ARDI server URL
        self.server = srv        
        
        #The web port (if custom)
        self.webport = port
        
        #The name/folder of the site
        self.site = site

        #The timezone of the server
        self.timezone = None

        #The schema for the URL
        self.prefix = "https://"
        if secure == False:
            self.prefix = "http://"
        self.contexts = []

        #Extract detail from the URL if the site was not given but is contained in the /s/
        bits = srv.split("/")
        if bits[0] == "":
            bits = bits[1:]
        if len(bits) > 1 and bits[1] == 's':
            self.site = bits[2]
            self.server = bits[0]        

    #Connect to the ARDI server
    def Connect(self):
        con = False

        #Attempt HTTPS...
        if self.prefix == "https://":
            url = "https://" + self.server + '/s/' + self.site + '/api/connect'            
            self.prefix = "https://"
            try:
                resp = requests.get(url)          
                if resp.status_code == 200:
                    con = True
                    if self.webport is None:
                        self.webport = "443"
                    self.server = self.server.replace(":80","")
            except:
                pass

        #If HTTPS connection fails...
        if con == False:            
            url =  "http://" + self.server + '/s/' + self.site + '/api/connect'                
            self.prefix = "http://"            
            if self.webport is None:
                self.webport = "80"
            resp = requests.get(url)

        # HTTP response code, e.g. 200.
        if resp.status_code == 200:            

            #print 'XML Content: ' + buffer.getvalue()
            xml = xmltodict.parse(resp.text)
            
            #Parse the details of the server
            for d in xml['ardi']['service']:                
                if d['@name'] == 'data':
                    prt = 5336
                    src = self.server
                    try:
                        prt = d['@port']
                    except:
                        pass
                    
                    try:
                        src = d['@host']
                    except:
                        pass                

                    ctx = Context()
                    ctx.consolidator= prt
                    ctx.server = src
                    ctx.name = "Actual"
                    self.contexts.append(ctx)            

            #Parse the details of the server
            for d in xml['ardi']['setting']:               
                if d['@name'] == 'timezone':
                    try:                        
                        self.timezone = pytz.timezone(d['#text'])
                    except:
                        print("Invalid Server Timezone")
                        traceback.print_exc()
                        self.timezone = pytz.utc

            return True
        else:
            #ar.close()
            return False        
        
    #Get key configuration data from the ARDI server
    def GetConfiguration(self):        
        url = self.prefix + self.server + ':' + str(self.port) + '/api/getconfiguration'
        resp = requests.get(url)

        # HTTP response code, e.g. 200.
        if resp.status_code == 200:
            #print buffer.getvalue()
            xml = xmltodict.parse(resp.text)

            rels = []
            props = []

            #Load relationship names
            for ele in xml['config']['relations']['relationship']:
                rels.append({ 'name': ele['@name'], 'id': ele['@id'] })                
            
            #Load property names
            for ele in xml['config']['properties']['property']:
                props.append({ 'name' : ele['@name'], 'type' : ele['@type'], 'id' : ele['@id'] })
        else:
            return None
                
        ar.close()        
        return [rels,props]

    #Get information about individual data sources
    def GetDataSourceInfo(self):        
        url = self.prefix + self.server + ':' + str(self.port) + '/api/getdatasources.php'
        resp = requests.get(url)        

        # HTTP response code, e.g. 200.
        if resp.status_code == 200:            
            xml = xmltodict.parse(resp.text)

            drivers = []
            dsources = []

            for ele in xml['config']['drivers']['driver']:
                drivers.append({ 'name': ele['@name'], 'port': ele['@port'], 'code' : ele['@code'] })
            
            for ele in xml['config']['datasources']['source']:
                dsources.append({ 'name' : ele['@name'], 'type' : ele['@type'], 'id' : ele['@id'], 'port' : ele['@port'] })
        else:
            return None
                
        ar.close()        
        return [drivers,dsources]

    #Return the full ARDI server URL
    def Endpoint(self):
        return self.prefix + self.server + ':' + str(self.webport) + "/s/" + self.site

    #Create an AQL query object
    def StartQuery(self):
        return AQLQuery(self)

    #Convert a local time to UTC
    def ToUTC(self,dt):        
        if self.timezone == None:
            print("WARNING: Unknown Time Zone. Old server, or not connected.")
            return dt
        dt = dt.replace(tzinfo=self.timezone)
        return dt.astimezone(pytz.utc).replace(tzinfo=None)

    #Convert a UTC time to local time
    def ToLocal(self,dt):
        if self.timezone == None:
            print("WARNING: Unknown Time Zone. Old server, or not connected.")
            return dt
        dt = dt.replace(tzinfo=pytz.utc)
        return dt.astimezone(self.timezone).replace(tzinfo=None)

#Convert an ARDI colour to a R/G/B tuple
def ParseHexColour(hx):
    if hx[0] == '#':
        hx = hx[1:]
    try:
        r = float.fromhex(hx[0:2])/255.0
    except:
        r = 0

    try:        
        g = float.fromhex(hx[2:4])/255.0
    except:
        g = 0

    try:
        b = float.fromhex(hx[4:6])/255.0
    except:
        b = 0

    return (r,g,b)
    
#An AQL history response
class AQLHistResponse:
    def __init__(self,dframe,metadata):
        self.data = dframe
        self.metadata = {}
        self.errors = []
        
        for results in metadata['results']:
            if results['type'] == 'pointlist':
                for v in results['value']:
                    nm = v['name'] + " " + v['propname']
                    v['history'] = None
                    self.metadata[nm] = v
                    
        if 'errors' in metadata:
            self.errors = metadata['errors']

    #Gets metadata about an individual column
    def GetColumnData(self,col):
        return self.metadata[col]

    #Converts a discrete column value to text
    def GetColumnText(self,name,val):
        try:
            if name in self.metadata:
                if 'map' in self.metadata[name]:
                    if type(self.metadata[name]['map']) == list:
                        return self.metadata[name]['map'][int(val)]
                    else:
                        return self.metadata[name]['map'][str(val)]
        except:
            pass
        
        return str(val)    

    #Converts an analogue or discrete value to a colour
    def GetColumnColour(self,name,val):        
        if name in self.metadata:
            if 'colours' in self.metadata[name]:                                    
                lastcolour = (0,0,0)
                lastvalue = None
                for x in self.metadata[name]['colours']:
                    fx = float(x)
                    if lastvalue is None:
                        lastcolour = ParseHexColour(self.metadata[name]['colours'][x])
                        
                    if fx > val:                        
                        break
                    
                    lastcolour = ParseHexColour(self.metadata[name]['colours'][x])
                    lastvalue = fx

                if lastvalue is None:                    
                    return lastcolour
                else:                    
                    nextcolour = ParseHexColour(self.metadata[name]['colours'][x])
                    if fx == lastvalue:
                        perc = 0
                    else:
                        perc = (val - lastvalue) / (fx - lastvalue)
                    return ((perc * nextcolour[0]) + ((1-perc) * lastcolour[0]),(perc * nextcolour[1]) + ((1-perc) * lastcolour[1]),(perc * nextcolour[2]) + ((1-perc) * lastcolour[2]))
                
        return "blue"

    #Returns a colour map from a property name (used for discrete properties)
    def GetColourMap(self,name):        
        if name in self.metadata:
            if 'colours' in self.metadata[name]:
                clrs = {}
                indx = -1
                for x in self.metadata[name]['colours']:
                    indx += 1
                    if str(x)[0] == '#':
                        clrs[int(indx)] = x
                    else:
                        clrs[int(x)] = self.metadata[name]['colours'][x]
                        
                return clrs
        return {}

    #Returns a value map from a property name (used for discrete properties)
    def GetValueMap(self,name):        
        if name in self.metadata:
            if 'map' in self.metadata[name]:
                if type(self.metadata[name]['map']) == list:
                    values = {}
                    for x in range(0,len(self.metadata[name]['map'])):
                        values[x] = self.metadata[name]['map'][x]
                    return values
                else:
                    return self.metadata[name]['map']
        return {}

#This is used to pass parameters for an AQL query
class AQLHistRequest:
    def __init__(self,query,args=None):
        self.query = query
        self.sd = datetime.datetime.now() - datetime.timedelta(hours=24)
        self.ed = datetime.datetime.now()

        if args is not None:
            self.serverzone = args.serverzone
            self.localzone = args.localzone
            self.sd = args.startdate
            self.ed = args.enddate        

        self.namemap = None
        self.serverzone = None
        self.localzone = None
        self.mapbad = None
        self.mapna = None
        self.autofill = True
        self.trim = True
        self.pad = True
        self.chunks = None
        self.samples = None
        self.span = None
        self.mode = "interp"
        self.context = 1

    #Sets the name of the 'local' timezone
    def SetLocalTimezone(self,tz):
        self.localzone = tz

    #Sets the start/end of the report, and optionally the number of 'chunks' to break it into
    def SetRange(self,start,end,chunks=None):
        self.sd = start
        self.ed = end
        self.chunks = chunks

    #Gets the start and end times as a tuple
    def GetTrim(self):
        return (self.sd,self.ed)

    #Gets the start and end times as formatted strings
    def GetRange(self):
        return [self.sd.strftime("%Y-%m-%d %H:%M:%S"),self.ed.strftime("%Y-%m-%d %H:%M:%S")]

    #Request the data without interpolation
    def Raw(self):
        self.mode = "raw"

    #Return the average values during each time window
    def Interpolated(self):
        self.mode = "interp"

    #Return the minimum values during each time window
    def Min(self):
        self.mode = "min"

    #Return the maximum values during each time window
    def Max(self):
        self.mode = "max"

#Represents an AQL query
class AQLQuery:
    def __init__(self,server):
        self.server = server

    #Run the AQL query
    def Execute(self,query):
        url = self.server.Endpoint() + "/api/aql/query"        
        req = requests.post(url,{ "query": query })    
        return req.json()

    #Return a fresh AQLHistRequest object based on the start and end times
    def StartHistoryRequest(self,query,start,end):
        r = AQLHistRequest(query)
        r.SetRange(start,end)
        return r

    #Creates a new AQLHistRequest object based on query arguments that match those from MPLReport
    def StartHistoryQuery(self,query,args):
        r = AQLHistRequest(query)
        #print(dir(args))
        r.SetRange(args.utcstart,args.utcend)
        r.localzone = args.local_zone
        r.serverzone = args.server_zone        
        return r

    #Convert a value to a floating point number if possible
    def cvFloat(self,dta):
        if dta == "^":
            return None
        else:
            try:
                v = float(dta)
                if v != "NaN":
                    return v
            except:
                return dta
        return dta

    #Convert a value to an integer value if possible
    def cvInt(self,dta):
        if dta == "^":
            return None
        else:
            v = int(dta)
            if v != "NaN":
                return v
        return dta

    #Convert a YYYY-MM-DD HH:MM:SS string to a LOCAL time
    def ConvertTZString(self,dt, fromtz, totz):       
        try:
            local = datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
        except:
            try:                
                local = datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S.%f")
            except:
                local = dt
                return None
            
        local = local.replace(tzinfo=fromtz)        
        
        return local.astimezone(totz).strftime("%Y-%m-%d %H:%M:%S")

    #Convert a DateTime to a LOCAL time
    def ConvertTZDate(self,dt, fromtz, totz):               
        local = dt.replace(tzinfo=fromtz)            
        return local.astimezone(totz).replace(tzinfo=None)

    #Get history from an AQLHistoryRequest
    def GetHistory(self,req,md=False):
        query = req.query
        if req.localzone is None:
            req.localzone = pytz.utc
            
        if req.serverzone is None:
            req.serverzone = pytz.utc        

        grain = 0
        if req.samples is None and req.span is None:
            grain = -100
        else:
            if req.samples is not None:
                grain = -req.samples
            else:
                grain = req.span        

        querystring = '{"start": %START%,"end": %END%, "grain": %GRAIN%, "method": "' + req.mode + '"'        
            
        if req.chunks is None:
            query = req.query.replace("{",querystring)
            query = query.replace("%START%",'"' + str(req.sd.strftime("%Y-%m-%d %H:%M:%S")) + '"')
            query = query.replace("%END%",'"' + str(req.ed.strftime("%Y-%m-%d %H:%M:%S")) + '"')
            query = query.replace("%GRAIN%",'"' + str(grain) + '"')
            
            results = self.Execute(query)
            if md == False:
                return self.HistoryToDataframe(results,namemap=req.namemap,mapbad=req.mapbad,mapna = req.mapna,autofill=req.autofill,pad=req.pad,trim=req.GetTrim(),serverzone = req.serverzone, localzone=req.localzone)
            return AQLHistResponse(self.HistoryToDataframe(results,namemap=req.namemap,mapbad=req.mapbad,mapna = req.mapna,autofill=req.autofill,pad=req.pad,trim=req.GetTrim(),serverzone = req.serverzone, localzone=req.localzone),results)
        else:
            chunkset = []
            curr = req.sd

            ttime = 0
            while curr < req.ed:
                dend = curr + (datetime.timedelta(seconds = (60*60*req.chunks)-1))
                if dend > req.ed:
                    dend = req.ed
                chunkset.append([curr,dend])
                ttime = ttime + (dend - curr).total_seconds()
                curr = curr + datetime.timedelta(hours=req.chunks)

            finaldf = None
            for chunk in chunkset:                
                chunkgrain = grain
                if chunkgrain < 0:
                    chunkgrain = int(grain * ((chunk[1] - chunk[0]).total_seconds() / ttime))
                
                query = req.query.replace("{",querystring)
                query = query.replace("%START%",'"' + str(chunk[0].strftime("%Y-%m-%d %H:%M:%S")) + '"')
                query = query.replace("%END%",'"' + str(chunk[1].strftime("%Y-%m-%d %H:%M:%S")) + '"')
                query = query.replace("%GRAIN%",'"' + str(chunkgrain) + '"')
                #print(query)                
                
                results = self.Execute(query)
                df = self.HistoryToDataframe(results,namemap=req.namemap,mapbad=req.mapbad,mapna = req.mapna,autofill=req.autofill,pad=req.pad,trim=chunk,serverzone = req.serverzone, localzone=req.localzone)
                #print("Frame Contains Data From " + str(df.index[0]) + " to " + str(df.index[len(df.index)-1]))
                if finaldf is None:
                    finaldf = df
                else:
                    finaldf = pd.concat([finaldf,df])
                    #finaldf = finaldf.append(df)

            if md == False:
                return finaldf
            else:
                return AQLHistResponse(finaldf,results)

    #Convert a list of AQL points to a Dataframe
    def pointlistToDataFrame(self,results):
        columns = []
        for q in results['results']:        
            if q['type'] == "pointlist":
                for r in q['value']:
                    columns.append(r['name'] + " " + r['propname'])
        return pd.DataFrame(columns=columns)
    
    #Convert AQL history to an interpolated/complete data frame
    def HistoryToDataframe(self,results,namemap=None,serverzone=None,localzone=None,mapbad=None,mapna=None,autofill=False,pad=True,trim=None):
        indx = -1
        frames = []
        interp = []        
        
        for q in results['results']:        
            if q['type'] == "pointlist":            
                for r in q['value']:                
                    indx = indx + 1
                    #Build a Pandas series from each JSON result
                    
                    #Get the history from the JSON
                    timeseries = None
                    try:
                        timeseries = r['history']
                    except:
                        pass                

                    if timeseries is None:
                        continue

                    #print(str(r))
                    if r['type'] == 'MEASUREMENT':
                        interp.append('cont')
                    else:
                        interp.append('discrete')

                    #Build the channel name
                    sname = r['name'] + " " + r['propname']
                    if namemap is not None:
                        try:
                            sname = namemap[indx]
                        except:
                            pass

                    #Get the time index, using the passed timezone if available.
                    if serverzone == None:
                        dindex = pd.DatetimeIndex([i[0] for i in timeseries])
                    else:
                        dindex = pd.DatetimeIndex([self.ConvertTZString(i[0],serverzone,localzone) for i in timeseries])

                    #Add this new series to the array
                    try:
                        if len(r['map']) > 0:
                            pass
                        frames.append(pd.DataFrame([self.cvInt(i[1]) for i in timeseries],columns=[sname],index=dindex))                   
                    except:
                        frames.append(pd.DataFrame([self.cvFloat(i[1]) for i in timeseries],columns=[sname],index=dindex))             

        #Build up the final dataframe
        final = None        

        findex = -1
        for n in frames:
            findex = findex + 1
            #Some value substitution has to be done here, on a per-channel basis, due to the addition
            # of 'NaN' values during join operations
            
            #Map specific I/O values as 'bad'
            if mapbad is not None:
                for x in mapbad:
                    if x[0] in n.columns:                    
                        n = n.replace(x[1],np.nan)

            #Map bad values as a specific value
            if mapna is not None:
                for x in mapna:                
                    if x[0] in n.columns:
                        if x[1] != 'hold' and x[1] != 'discrete' and x[1] != 'interp' and x[1] != 'cont':                            
                                n.fillna(value=x[1],inplace=True)
            
            #Combine the series into a data frame 
            if final is None:
                final = n.fillna(value=np.nan)
            else:
                n = n.fillna(value=np.nan)
                final = final.join(n,how='outer',lsuffix="",rsuffix="_dup")
                final = final.groupby(level=0).min()        

        #If no history was available, make up a dataframe from the point list data
        if final is None:        
            return self.pointlistToDataFrame(results)

        #Eliminate duplicate indexes
        #print(str(final))
        final = final.groupby(level=0).min()
        
        findex = -1
        for col in final.columns:
            findex = findex + 1
            if mapna is not None:
                for x in mapna:
                    if x[0] == str(col):
                        if x[1] == 'hold' or x[1] == 'discrete':
                            final[col] = final[col].bfill()
                            final[col] = final[col].ffill()                           
                        else:
                            if x[1] == 'interp' or x[1] == 'cont':
                                final[col] = final[col].interpolate()
                                final[col] = final[col].bfill()
                                final[col] = final[col].ffill()                

            if autofill == True:
                try:
                    if interp[findex] == 'interp' or interp[findex] == 'cont':
                        final[col] = final[col].interpolate()                    
                except:
                    pass

            if autofill == True:
                final[col] = final[col].bfill()
                final[col] = final[col].ffill()        
        
        #Pad the start and end dates into the frame if not available
        if trim is not None:
                        
            rs = trim[0].replace(tzinfo=None,microsecond=0)
            re = trim[1].replace(tzinfo=None,microsecond=0)

            rs = self.ConvertTZDate(rs,serverzone,localzone)
            re = self.ConvertTZDate(re,serverzone,localzone)

            trimmed = final[rs:re]
            if len(trimmed.index) > 1:
                
                si = trimmed.index[0]
                ei = trimmed.index[-1]

                if si != rs:
                    cols = []
                    for cl in final.columns:
                        cols.append(str(cl))
                    dindex = pd.DatetimeIndex([rs])                
                    mod = pd.DataFrame([final.iloc[0].values],index=dindex,columns=cols)                
                    #trimmed = trimmed.append(mod)
                    trimmed = pd.concat([trimmed,mod],axis=0)
                    trimmed.sort_index(inplace=True)                

                if ei != re:                
                    cols = []
                    for cl in final.columns:
                        cols.append(str(cl))
                    dindex = pd.DatetimeIndex([re])
                    mod = pd.DataFrame([final.iloc[-1].values],index=dindex,columns=final.columns)
                    #trimmed = trimmed.append(mod)            
                    trimmed = pd.concat([trimmed,mod],axis=0)

                final = trimmed
        
        return final

#Represents a live connection to ARDI data
class Subscription:
    def __init__(self,core):
        self.core = core
        self.subscription = ""
        self.codes = []
        self.cancelled = False
        self.codechange = False
        self.threaded = True
        self.callback = None
        self.context = None
        self.closed = False

    #Adds a new ARDI point to the subscription
    def AddCode(self,address):
        self.codes.append(address)
        self.codechange = True

    #Connect to live data
    def Connect(self):
        self.ThreadBody()

    #Disconnect from live data
    def Disconnect(self):
        self.cancelled = True

    #Internal: Initial live data subscription
    def Subscribe(self):
        self._call("subscribe")
        self.codechange = False
        if self.subscription != "":
            return True
        
        return False
    
    #Set the callback function that is called with fresh data
    def SetCallback(self,call,cont):
        self.callback = call
        self.context = cont

    #Internal: Unsubscribe from live data
    def Unsubscribe(self):
        self._call("unsubscribe")
        self.subscription = ""
        pass

    #Disconnect from live data & clear subscriptions
    def Clear(self):
        if self.subscription != "":
            self.Unsubscribe()
        self.codes = []

    #Performs an internal live data update, re-connecting if the subscription list has changed.
    def Update(self):
        if self.codechange == True:
            self.Unsubscribe()
            self.Subscribe()
            return
        self._call("update")
        pass
    
    #Handle the long-polling request for live data
    def _call(self,function):
        
        if (len(self.codes) == 0):
            time.sleep(1)
            return

        try:            
            fullurl = self.core.server
            try:
                ps = fullurl.index(':')
                if ps > -1:
                    fullurl = fullurl[0:ps]
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                pass

            try:
                ps = fullurl.index('/')
                if ps > -1:
                    fullurl = fullurl[0:ps]
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                pass            

            fullurl = "http://" + fullurl
            fullurl += ":" + self.core.contexts[0].consolidator
            fullurl += "/" + function

            if self.core.prefix == "https://":
                #Use HTTPS Proxy
                addr = "https://" + self.core.servername + "/data/livedata?format=json&port=" + this.dataport + "&action=" + function;                            

            anydata = False
            post_data = {}
            if function != "subscribe":
                post_data['id'] = self.subscription
                anydata = True

            if function == "subscribe":                
                codelist = ""
                for itm in self.codes:                    
                    if codelist != "":
                        codelist = codelist + ","
                    codelist += itm
                    anydata = True
                post_data['codes'] = codelist
                anydata = True

            if anydata == True:
                postfields = urlencode(post_data)
            try:
                
                if function == "subscribe":                    
                    r = requests.post(fullurl,data={'codes': codelist,'format': 'json' }, timeout=5)                    
                else:
                    r = requests.post(fullurl,data={'id': self.subscription,'format': 'json' }, timeout=30)                
                
                returned = {}

                js = {}
                try:
                    js = r.json()
                except:
                    if function != "subscribe":
                        self._call("subscribe")
                        return True
                    
                self.subscription = js['id']

                for itm in js['items']:
                    cd = itm['code']                    
                    returned[cd] = itm['value']
                                   
                if self.callback is not None:
                    self.callback(returned,self.context)            
            except (KeyboardInterrupt, SystemExit):
                self.cancelled = True
                return False
            except:
                print("Failed To Send!")
                traceback.print_exc()
                return False
            return True
        
        except:
            traceback.print_exc()
            return False

    #Main thread body
    def ThreadBody(self):
        while self.cancelled == False:
            while self.subscription == "":
                if self.Subscribe() == False:
                    try:
                        time.sleep(5)
                    except:
                        self.cancelled = True
                    if self.cancelled == True:
                        break;
                else:
                    break
            if self.cancelled == True:
                break;

            if self.Update():
                #Call the callback function with our new data...                
                time.sleep(1)
                pass
            else:
                #No new data arrived - immediately try again.
                time.sleep(0.5)
                pass

#Represents a single ARDI live channel
class Channel:
    def __init__(self,session):        
        self.filters = None
        self.type = ""
        self.code = ""
        self.value = None
        self.properties = {}
        self.session = session

    def SetValue(self,val):        
        self.value = val

    def AsText(self):
        return str(self.value)

    def AsFloat(self):
        return float(self.value)

    def AsFull(self):
        return self.AsText()

    def __repr__(self):
        return self.AsText()

#An more user-friendly variant of the Subscription that accepts human-readable point names.
class Session:
    def __init__(self,server):
        self.server = server
        self.rawchannels = []
        self.channels = []
        self.mapping = {}
        self.subscription = None
        self.callbackfunction = None

    #Add an individual channel by name and property
    def AddChannel(self,asset,prop=None):
        query = AQLQuery(self.server)
        if prop is None:
            channels = [self._getChannelForNode(asset)]
        else:
            js = query.Execute("'" + asset + "' ASSET '" + prop + "' PROPERTY VALUES")
            channels = self._getChannelsFromAQL(js)

        if len(channels) > 0:
            channel = channels[0]
            self.channels.append(channel)
            return channel
        else:
            return None

    def AddPoint(self,asset,prop):
        query = AQLQuery(self.server)
        js = query.Execute(str(asset) + " ASSETBYID " + str(prop) + " PROPERTYBYID VALUES")
        channels = self._getChannelsFromAQL(js)
        if len(channels) > 0:
            channel = channels[0]
            self.channels.append(channel)
            return channel
        else:
            return None

    def _getChannelsFromAQL(self,js):
        points = self._extractPointsFromAQL(js)
        return self._getChannelsForPoints(points)

    def _getChannelForNode(self,ast):
        
        bits = ast.split(':')
        assetid = bits[0]
        prop = bits[1]        
        query = AQLQuery(self.server)
        js = query.Execute(str(assetid) + " ASSETBYID " + str(prop) + " PROPERTYBYID VALUES")
        points = self._extractPointsFromAQL(js)        
        return self._getChannelsForPoints(points)[0]        

    def _getChannelsForPoints(self,points):
        channels = []
        for pnt in points:
            node = None
            chan = Channel(self)
            chan.name = pnt['name'] + " " + pnt['propname']
            chan.value = pnt['value']
            if pnt['type'] == 'MEASUREMENT':
                node = "measurement"
                chan.properties["min"] = pnt['min']
                chan.properties["max"] = pnt['max']
                chan.properties["units"] = pnt['units']
            if pnt['type'] == 'STATUS':
                node = "state"
            if pnt['type'] == 'LOOKUP':
                node = "text"
            if pnt['type'] == 'TEXT':
                node= "text"
            if pnt['type'] == 'ENUM':
                node = "value"

            if node is not None:
                chan.code = str(pnt['sourceid']) + ":" + str(pnt['propid']) + ":" + node
                #print(str(chan.code))

            channels.append(chan)

        return channels

    def _extractPointsFromAQL(self,dct):
        points = []
        #print("Dictionary: " + str(dct))
        for reslist in dct['results']:
            if reslist['type'] != 'pointlist':
                continue
            for pnt in reslist['value']:
                points.append(pnt)
        return points

    def _dataupdates(self,updates,context):
        updated = []
        #print("New Data Updates Arrived: " + str(updates))
        for x in updates:
            try:
                #print("Searching For " + x + " in" + str(self.mapping))
                for q in self.mapping[x]:
                    q.SetValue(updates[x])
                for v in self.mapping[x]:
                    updated.append(v)
            except:
                traceback.print_exc()
                pass
        if len(updated) > 0:
            if self.callbackfunction != None:
                self.callbackfunction(updated)

    #Add multiple channels by AQL query
    def AddChannels(self,qry):
        query = AQLQuery(self.server)
        js = query.Execute(qry)

        channels = self._getChannelsFromAQL(js)        

        response = []
        for q in channels:        
            self.channels.append(q)
            response.append(q)
        
        return response

    #Add multiple channels from a list of 'Asset.Property' strings
    def AddChannelList(self,lst):
        url = self.server.Endpoint() + "/api/lookuppoints"
        resp = requests.post(url,{"points": ";".join(lst), "format": "json"})

        #print("Lookup Results: " + resp.text)
        dta = json.loads(resp.text)

        channels = []
        for pnt in dta:
            chan = Channel(self)
            chan.name = pnt['name']
            chan.value = 0
            chan.code = pnt['code']
            if 'min' in pnt:
                chan.properties["min"] = pnt['min']
            if 'max' in pnt:
                chan.properties["max"] = pnt['max']
            if 'units' in pnt:
                chan.properties["units"] = pnt['units']
            
            channels.append(chan)

        for q in channels:        
            #print("Adding " + str(q.name) + " / " + str(q.code))
            self.channels.append(q)           

        return channels

    def Callback(self,func):
        self.callbackfunction = func
        
    #Connect and start processing
    def Start(self):
        self.subscription = Subscription(self.server)
        for n in self.channels:
            if n.code != "":
                #print("Subscribing To: " + n.code)
                self.subscription.AddCode(n.code)
                if n.code not in self.mapping:
                    self.mapping[n.code] = []
                self.mapping[n.code].append(n)

        self.subscription.SetCallback(self._dataupdates,None)
        self.subscription.Connect()        
        return True

    #Disconnect
    def Stop(self):
        if self.subscription is not None:
            self.subscription.Disconnect()
            self.subscription = None
