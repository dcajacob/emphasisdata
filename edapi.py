    # 11/22/11 demonstrate emphasisdata.com API commmunication

import os, sys, socket
from struct import *             # struct allows packing and unpacking for socket communication
from time import sleep
from datetime import datetime as dt
import traceback

import pandas as pd
from pandas import Series, TimeSeries, DataFrame

class EmphasisData(object):

    sapi = None
    timeout = 5.0

    def __init__(self, host='localhost', port=7729):
        """Create an instance of a Prologix GPIB Ethernet Widget
        """

        self._address = None

        self.host = host
        self.port = port
        self._isOpen = False

    def connect(self):
        """Create a connection to a Prologix GPIB Ethernet Widget
        """

        # Open TCP connection to GPIB-ETHERNET
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        self.sock.settimeout(self.timeout)
        self.sock.connect((self.host, self.port))

        if self.sock.send('\1') != 1:               # check for valid connection
            print "send 1 error"
            self.close()
        ret = ord(self.sock.recv(1)[0])
        if ret == 0:
            print "connected to API"
        else:
            print "connection error"
            self.close()

        self._isOpen = True

    def close(self):
        """Close the connection to the Prologix GPIB Ethernet Widget
        """

        self.sock.close()
        self._isOpen = False

    def RecvAll(self, size=280):

        total_len = 0
        total_data = []

        size_data = sock_data = ''
        recv_size = len

        while total_len < size:
            sock_data = self.sock.recv(size, socket.MSG_WAITALL)
            if not len(total_data):
                if len(sock_data) > 0:
                    recv_size = size - len(sock_data)
                    total_len = total_len + len(sock_data)
                    total_data.append(sock_data)
                sleep(0.00001)
        return ''.join(total_data)

    def getSecurities(self):
        """Return security_id based DataFrame of all sid's"""

        exchange = {0:'NASDAQ', 1:'NYSE', 2:'ASE', 6:'OTC'}

        # Request number of securities in database
        if not self.sock.send('\3'):
            print "send 3 error"
            self.close()
            return False

        ninfo = unpack('I',self.RecvAll(size=4))[0]
        print "%d possible security_id's" % ninfo
        Info = {}                               # empty dictionary
        sid = 0

        # Request the list of securities
        if not self.sock.send('\4'):
            print "send 4 error"
            self.close()
            return False

        sids = []; tickers = []; ciks = []; sics = []; xchngs = []; names = []

        while sid != 9999999:
            info = self.RecvAll(size=280)
            if len(info) != 280:
                print "info recv error, only %d bytes" % len(info)
                self.close()
                return False

            sid,cik,sic,xchg,name,tkr = unpack('2I1i1I256s8s',info)
            name = name.split("\0",1)[0]          # remove garbage after null byte
            tkr = tkr.split("\0",1)[0]
            #Info[sid] = {'ticker':tkr, 'cik':cik, 'sic':sic, 'exchange':exchange[xchg], 'company':name}   # add dictionary item

            sids.append(sid)
            tickers.append(tkr)
            ciks.append(cik)
            sics.append(sic)
            xchngs.append(exchange[xchg])
            names.append(name)

        #assert list(set(sid)) == sid # SID list should be unique
        info = {'ticker':tickers, 'cik':ciks, 'sic':sics, 'exchange':xchngs, 'company':names}
        universe = pd.DataFrame(info, index=sids)

        print "%d entries in security_id Info dictionary" % len(universe)
        return universe

    def DataQuery(self, date, query):
        """return list of >0 query results as (security_id,value) tuples"""

        nq = len(query)
        packfmt = '=BII%ds' % nq
        qapi = pack(packfmt, 2, nq,date, query)
        if self.sock.send(qapi) != len(qapi):       # send the query
            print "send query api error"
            self.close()
            return False

        Result = []                          # empty list
        ACK = ord(self.sock.recv(1)[0])           # receive ACK byte
        if ACK != 0:
            print "query error"
            return Result
        nret = unpack('I', self.sock.recv(4))[0]   # this is the number of results we'll receive
        for k in range(nret):                # each result is a (security_id,value) tuple
            Result.append(unpack('If', self.sock.recv(8)))

        return Result

    def GetLatestBusinessDay(self):

        lastbday = pd.bdate_range(start=dt.now() - pd.DateOffset(months=1), end=dt.now())[-1]
        return lastbday

    def Query(self, query, date=None):
        """"""

        sids = []
        values = []

        if date is None:
            date = self.GetLatestBusinessDay()

        print type(date), date

        date = date.strftime('%Y%m%d')

        result = self.DataQuery(int(date), query)

        if len(result) == 0:
            print "No results"

        for sid, val in result:
            if val:
                #tkr,cik,sic,xchg,name = Info[sid]
                sids.append(int(sid))
                values.append(float(val))

        result = Series(data=values, index=pd.Index(sids, name='sid'))
        result.index.name = 'sid'

        return result

if __name__ == '__main__':

    # ------------------------- MAIN ------------------------------

    try:
        s = raw_input("\nEmphasisData API port (blank=default 7729): ").strip()
        if len(s)>0:
            PORT = int(s)
        else:
            PORT = 7729

        sapi = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sapi.connect(('localhost', PORT))
        sapi.settimeout(5.0)    # max 10 sec timeout so python doesn't hang

        if not sapi.send('\1'): # check for valid connection
            print "send 1 error"
            CloseApi()

        ret = ord(sapi.recv(1)[0])
        if ret == 0:
            print "connected to API"
        else:
            print "connection error"
            CloseApi()

        Info = EmphasisGetInfoDict()    # collect all the security_id Info at the start

        while 1:  # loop to execute boolean queries
            s = raw_input("\nYYYYMMDD query (blank=exit): ").strip()
            if len(s) < 1:
                CloseApi()
            date,query = s.split(None,1)
            Result = EmphasisDataQuery(int(date),query)
            if len(Result)==0:
                print "  no results"
                continue

            sids, values = zip(*Result)
            Results = pd.DataFrame({'values': values}, index=pd.Index(sids, name='sid'))
            print Results.head()
            #for sid,val in Result:
            #    if val:
            #        #tkr,cik,sic,xchg,name = Info[sid]
            #        print "  %-6s %8d %s %s" % (Info.xs(sid)['ticker'],int(Info.xs(sid)['cik']),Info.xs(sid)['company'],str(val))
    except KeyboardInterrupt:
        sapi.close()
    except Exception as e:
        sapi.close()
        print(traceback.format_exc())
