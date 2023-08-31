import requests
import xmltodict
import pandas as pd
import numpy as np
import json
import traceback
import time
from dateutil import tz

try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode

class Context:
    def __init__(self):
        self.consolidator = 5336
        self.submission = 2991
        self.name = "Actual"
        self.server = ""

class Server:
    def __init__(self, srv, site='default', port=80):
        self.server = srv        
        self.webport = port
        self.site = site
        self.contexts = []

    def Connect(self):
        url =  'http://' + self.server + '/s/' + self.site + '/api/connect'
        resp = requests.get(url)

        # HTTP response code, e.g. 200.
        if resp.status_code == 200:            

            #print 'XML Content: ' + buffer.getvalue()
            xml = xmltodict.parse(resp.text)
            
            for d in xml['ardi']['service']:
                #print 'Checking ' + d['@name']
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
            return True
        else:
            #ar.close()
            return False        
        

    def GetConfiguration(self):        
        url = 'http://' + self.server + ':' + str(self.port) + '/api/getconfiguration.php'
        resp = requests.get(url)

        # HTTP response code, e.g. 200.
        if resp.status_code == 200:
            #print buffer.getvalue()
            xml = xmltodict.parse(resp.text)

            rels = []
            props = []

            for ele in xml['config']['relations']['relationship']:
                rels.append({ 'name': ele['@name'], 'id': ele['@id'] })                
            
            for ele in xml['config']['properties']['property']:
                props.append({ 'name' : ele['@name'], 'type' : ele['@type'], 'id' : ele['@id'] })
        else:
            return None
                
        ar.close()        
        return [rels,props]

    def GetDataSourceInfo(self):
        url = 'http://' + self.server + ':' + str(self.port) + '/api/getdatasources.php'
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

    def Endpoint(self):
        return 'http://' + self.server + ':' + str(self.webport) + "/s/" + self.site

class AQLQuery:
    def __init__(self,server):
        self.server = server

    def Execute(self,query):
        url = self.server.Endpoint() + "/api/aql/query"
        #print("Requesting: " + url + " / " + str(query))
        req = requests.post(url,{ "query": query })    
        return req.json()
        #print("Content: " + str(content))
        #return json.loads(content)

    def Execute_DF(self,query,namemap=None,serverzone=None,localzone=None,mapbad=None,mapna=None,autofill=False,pad=True):
        results = self.Execute(query)
        return self.HistoryToDataframe(query,namemap,mapbad,mapna,autofill,pad)

    def HistoryToDataframe(self,results,namemap=None,serverzone=None,localzone=None,mapbad=None,mapna=None,autofill=False,pad=True):
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
                        dindex = pd.DatetimeIndex([ConvertTZ(i[0],serverzone,localzone) for i in timeseries])

                    #Add this new series to the array
                    try:
                        if len(r['map']) > 0:
                            pass
                        frames.append(pd.DataFrame([cvInt(i[1]) for i in timeseries],columns=[sname],index=dindex))                   
                    except:
                        frames.append(pd.DataFrame([cvFloat(i[1]) for i in timeseries],columns=[sname],index=dindex))     

        #Build up the final dataframe
        final = None

        #print(str(frames))

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
                final = final.join(n,how='outer')
                final = final.groupby(level=0).mean()

        #print(str(final.columns))

        #If no history was available, make up a dataframe from the point list data
        if final is None:        
            return pointlistToDataFrame(results)

        #Eliminate duplicate indexes
        final = final.groupby(level=0).min()

        findex = -1
        for col in final.columns:
            findex = findex + 1
            if mapna is not None:
                for x in mapna:
                    if x[0] == str(col):
                        if x[1] == 'hold' or x[1] == 'discrete':
                            final[col].fillna(method='bfill',inplace=True)
                            final[col].fillna(method='ffill',inplace=True)
                        else:
                            if x[1] == 'interp' or x[1] == 'cont':
                                final[col] = final[col].interpolate()
                                final[col].fillna(method='bfill',inplace=True)
                                final[col].fillna(method='ffill',inplace=True)                        

            if autofill == True:
                try:
                    if interp[findex] == 'interp' or interp[findex] == 'cont':
                        final[col] = final[col].interpolate()                    
                except:
                    pass

        if autofill == True:
            final.fillna(method='ffill',inplace=True)
            final.fillna(method='bfill',inplace=True)

        #Pad the start and end dates into the frame if not available
        if report is not None and pad==True:
            si = final.index[0]
            ei = final.index[-1]

            rs = report.localstart.replace(tzinfo=None)
            re = report.localend.replace(tzinfo=None)

            if si != rs:
                            
                #print("Need to pad start - " + str(si) + " vs " + str(rs))
                cols = []
                for cl in final.columns:
                    cols.append(str(cl))
                dindex = pd.DatetimeIndex([rs])                
                mod = pd.DataFrame([final.iloc[0].values],index=dindex,columns=cols)                
                final = final.append(mod)
                final.sort_index(inplace=True)                

            if ei != re:
                #print("Need to pad end - " + str(ei) + " vs " + str(re))
                cols = []
                for cl in final.columns:
                    cols.append(str(cl))
                dindex = pd.DatetimeIndex([re])
                mod = pd.DataFrame([final.iloc[-1].values],index=dindex,columns=final.columns)
                final = final.append(mod)            
        
        return final

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

    def AddCode(self,address):
        self.codes.append(address)
        self.codechange = True

    def Connect(self):
        self.ThreadBody()

    def Disconnect(self):
        self.cancelled = True

    def Subscribe(self):
        self._call("subscribe")
        self.codechange = False
        if self.subscription != "":
            return True
        
        return False
    
    def SetCallback(self,call,cont):
        self.callback = call
        self.context = cont

    def Unsubscribe(self):
        self._call("unsubscribe")
        self.subscription = ""
        pass

    def Clear(self):
        if self.subscription != "":
            self.Unsubscribe()
        self.codes = []

    def Update(self):
        if self.codechange == True:
            self.Unsubscribe()
            self.Subscribe()
            return
        self._call("update")
        pass
    
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

            #print("Making Remote Call: " + fullurl)

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
                    #print("Sending Codes: " + str({'codes': codelist,'format': 'json' }))
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
                #print("Updated!")
                time.sleep(1)
                pass
            else:
                #No new data arrived - immediately try again.
                time.sleep(0.5)
                pass
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

class Session:
    def __init__(self,server):
        self.server = server
        self.rawchannels = []
        self.channels = []
        self.mapping = {}
        self.subscription = None
        self.callbackfunction = None

    #Add an individual channel by name and property
    def AddChannel(self,asset,prop):
        query = AQLQuery(self.server)
        js = query.Execute("'" + asset + "' ASSET '" + prop + "' PROPERTY VALUES")
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
                print(str(chan.code))

            channels.append(chan)

        return channels

    def _extractPointsFromAQL(self,dct):
        points = []
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

    def Callback(self,func):
        self.callbackfunction = func
        
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
