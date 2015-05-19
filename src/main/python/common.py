from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol

from accumulo import AccumuloProxy
from accumulo.ttypes import *

__all__ = ['client','login','connect','scan','printScan','singleRowRange','batchScan']
client = None
login = None

def connect():
    global client
    global login
    
    transport = TSocket.TSocket('localhost', 42424)
    transport = TTransport.TFramedTransport(transport)
    protocol = TCompactProtocol.TCompactProtocol(transport)
    client = AccumuloProxy.Client(protocol)
    transport.open()

    login = client.login('root', {'password':'password'})

def scan(table, start, end, cols=None):
    
    # convert cols
    scols = []
    if not cols is None:
        for col in cols:
            parts = col.split(':')
            if len(parts) == 1:
                scols.append(ScanColumn(parts[0], ''))
            else:
                scols.append(ScanColumn(parts[0], parts[1]))
    
    opts = ScanOptions(range=Range(Key(row=start), True, Key(row=end), False), columns=scols)
    cookie = client.createScanner(login, table, opts)
    batch = client.nextK(cookie, 100)
    
    results = []
    while(len(batch.results) > 0):
        results.extend(batch.results)    
        batch = client.nextK(cookie, 100)
        
    return results
    
def printScan(table, start, end, cols=None):
    opts = ScanOptions(range=Range(Key(row=start), True, Key(row=end), False), columns=cols)
    cookie = client.createScanner(login, table, opts)
    batch = client.nextK(cookie, 100)
    while(len(batch.results) > 0):
        for entry in batch.results:
            print '\t'.join([entry.key.row, entry.key.colFamily, entry.key.colQualifier, entry.value])
        batch = client.nextK(cookie, 100)

def singleRowRange(rowId):
    return Range(Key(row=rowId), True, Key(row=rowId + '\0'), False)

def batchScan(table, items, cols=None):
    opts = BatchScanOptions(ranges=[singleRowRange(x) for x in items], columns=cols)
    cookie = client.createBatchScanner(login, table, opts)
    batch = client.nextK(cookie, 100)
    
    results = []
    while(len(batch.results) > 0):
        results.extend(batch.results)    
        batch = client.nextK(cookie, 100)
        
    return results


    