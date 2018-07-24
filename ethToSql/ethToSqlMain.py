from tqdm import tqdm
tqdm.monitor_interval = 0
import requests
import json
import datetime
import pandas as pd
import numpy as np
from .SeqlDB import SeqlDB
from . import EtherScamHTMLParser as esParser


class ethToSql():
    
    def __init__(self, EthHost, EthPort, dbConString):
        self.rpc_port=EthPort

        self.host= EthHost
        self.delay=0.0001
        self.url = "{}:{}".format(EthHost, EthPort)
        self.seqldb = SeqlDB(dbConString)
        
        #desired order for the contract dataframe
        self.dfContractColOrder = ['blockNumber', 'transactionHash', 'contractAddress', 
                              'creator', #instead of "from" 
                              'gasUsed', 'transactionIndex', 'cumulativeGasUsed']
        
        #desired order for the Transactions dataframe
        self.dfTransColsOrder = ['blockNumber', 'hash', 'transactionIndex', 'from', 'to', 
                            'contractCreated', #aux field: determines the adress of the contract created on that transaction
                            'valEth',    #aux field: value in Ether
                            'valFinney', #aux field: value in Finney
                            'valSzabo',  #aux field: value in Szabo
                            'value', 'gas', 'gasPrice', 'nonce'
                            #, 'input', 'r', 's', 'v' #dont want these
                            ]
        
        #desired order for the Bloc dataframe
        self.dfBlockColsOrder = ['number',
                            'transCount',     #aux field: number of transactions on that block
                            'uniqueAccounts', #aux field: accounts on that block
                            'contractCount',  #aux field: contracts created on the block
                            'hash',  'parentHash', 'miner', 'nonce',  'timestamp', 
                            'difficulty', 'totalDifficulty', 'gasLimit', 'gasUsed', 
                            'receiptsRoot', 'stateRoot', 'transactionsRoot', 
                            'sha3Uncles', 'size'
                            #, 'extraData', 'logsBloom', 'mixHash'
                            ,'alias'         #aux field: alias of the block
                            ,'hasAccountBalanceInfo'
                            ]
        self.dfBalColsOrder  = ['blockNumber', 'account', 'balEth', 'balFinney', 'balSzabo', 'balance']


    def makeRpcRequest(self, method, params, key='result', silent = False):
        headers = {"content-type": "application/json"}
            
        payload = {
            "method": method,
            "params": params,
            "jsonrpc": "2.0",
            "id": 0
        }
    
        res = requests.post(
              self.url,
              data=json.dumps(payload),
              headers=headers).json()
    
        if 'error' in res:
            if not silent:
                print (res['error']['message'])
            return None
        
        if key == None:
            return res
        else:
            return res[key]


    def hexToInt(self, _hex):
        '''
        Converst input to integer.
        Input can be in the hexadecimal or string format
        Ex: 
            hexToInt(0x1957e2)   -> 1660898
            hexToInt('0x1957e2') -> 1660898
        '''
        if isinstance(_hex, str):
            return int(_hex, 16)
        else:
            return int(_hex)
    
    def intToHex(self, _int):
        return hex(_int)
    
    def parseBlock(self, bnum, alias=None, getBalanceInfo=0, SAVE_TO_DB = True, printAtEnd = 0):
       
        if alias == None:
            alias = ''
            
        contractCount = 0  
            
        block = self.makeRpcRequest("eth_getBlockByNumber", [self.intToHex(bnum), True])
        transactions = block['transactions'] #the list of transactions returns everything eth_getTransactionByHash would return
    
        ########################################## Transactions
        dfTrans = pd.DataFrame()
        for tran in transactions:
            df = pd.DataFrame({'blockNumber': [bnum]})
            contractCreated = None #most of the transactions dont crete contracts
            
            if tran['to'] == None: ##Contract creation
                contractCount += 1
                contract =  self.makeRpcRequest("eth_getTransactionReceipt", [tran['hash']])
                contractCreated = contract['contractAddress']#Will store the contract created on the Transaction Table as well
                dfContract = pd.DataFrame({'blockNumber': [bnum]})
                for c in contract: #loop trough the "columns" of the contract
                    if c in('blockHash', 'to', 'logs', 'logsBloom', 'root'): #dont need / empty / dont care(?)
                        continue
                    val = contract[c]
                    if c in ('blockNumber', 'gasUsed', 'transactionIndex', 'cumulativeGasUsed'):
                        val = self.hexToInt(val)
                    dfContract[c] = val
                    
                dfContract.rename(columns={'from':'creator'}, inplace=True)
                dfContract = dfContract[self.dfContractColOrder]
                dfContract.to_sql('Contract', self.seqldb.seqlEngine, if_exists='append', index=False) if SAVE_TO_DB else None
            #else: #print ('Populate "is to contract" field?') # would need a list of contracts - need to populate the DB in order
            df['contractCreated'] = contractCreated
    
    
            for k in tran:
                if k == 'blockHash': #already parsed' # do not use "in" because "s" is a property and it is in "blockHash"
                    continue
                val = tran[k]
                if k in ('blockNumber', 'transactionIndex', 'value', 'gas', 'gasPrice', 'nonce'):
                    val = self.hexToInt(val)
    
                if k == 'value':
                    val = float(val)
                    df['valEth']    = round(val/10e17, 6)
                    df['valFinney'] = round(val/10e14, 6)
                    df['valSzabo']  = round(val/10e11, 6)
                    
                df[k] = val
                
            dfTrans = dfTrans.append(df)
    
        
        transCount = len(dfTrans)
    
        if transCount >0:
            dfTrans  = dfTrans [self.dfTransColsOrder] #'input', 'r', 's', 'v' are being ignored
            dfTrans.to_sql('BlockTransaction', self.seqldb.seqlEngine, if_exists='append', index=False) if SAVE_TO_DB else None
    
        ########################################## Accounts' balances per block
        uniqueAccounts = 0
        if transCount >0:
            dfBalances = pd.DataFrame()
            accounts = np.unique(#in case acc is on to and from
                            np.concatenate((
                                np.unique(dfTrans['from']),
                                np.unique(dfTrans['to'].dropna()
                                )), axis=0))
    
    
            uniqueAccounts = len(accounts)
            for a in accounts:
                self.seqldb.execute("exec [insertIfNotExistAccAlias] '{}','{}'".format(a, 'other'))
                            
            if getBalanceInfo == 1:
                for acc in accounts:
                    df = pd.DataFrame({'blockNumber': [bnum], 'account':[acc]})
                    balance = self.makeRpcRequest("eth_getBalance", [acc, self.intToHex(bnum)], silent=True)
                    if balance != None:
                        balance = float(self.hexToInt(balance))
                        df['balEth']    = round(balance/10e17, 6)
                        df['balFinney'] = round(balance/10e14, 6)
                        df['balSzabo']  = round(balance/10e11, 6)
                        df['balance']   = balance
                    dfBalances = dfBalances.append(df)
                   
                if len(dfBalances) >0 and len(dfBalances.columns)>2:
                    dfBalances = dfBalances[self.dfBalColsOrder]
                    dfBalances.to_sql('AccountBalances', self.seqldb.seqlEngine, if_exists='append', index=False) if SAVE_TO_DB else None
    
        ########################################## BLOCK
        dfBlock = pd.DataFrame({'number': [bnum]})    
        for k in block:
            if k in ('transactions', 'uncles'): #already parsed
                continue
            val = block[k]
            if k == 'timestamp':
                val = datetime.datetime.fromtimestamp(self.hexToInt(val))
            elif k in ('difficulty', 'totalDifficulty', 'gasLimit', 'gasUsed', 'size', 'number'):
                val = self.hexToInt(val)
            
            if k =='totalDifficulty':
                val=0 # fix the bigint error
                
            dfBlock[k] = val
        
        #AUX fileds:
        dfBlock['transCount']     = transCount
        dfBlock['uniqueAccounts'] = uniqueAccounts
        dfBlock['contractCount']  = contractCount
        dfBlock['alias']  = alias
        dfBlock['hasAccountBalanceInfo'] = getBalanceInfo
        
        dfBlock = dfBlock[self.dfBlockColsOrder]
        dfBlock.to_sql('Block', self.seqldb.seqlEngine, if_exists='append', index=False) if SAVE_TO_DB else None
        if printAtEnd == 1:
            print ('Finished block {} with: {} transactions, {} unique accounts and {} contracts created'.format(
                    bnum, transCount, uniqueAccounts, contractCount))
    

    def getCurrentListOfBlocks(self):
        df = self.seqldb.execute('select number from block')
        l = df['number'].tolist()
        return l
    
    
    def CleanAllFailedBlocks(self, alias=None, sql = None, parseAgain=True):
        if sql == None:
            dfFailed = self.seqldb.execute('select * from failures')
        else:
            dfFailed = self.seqldb.execute(sql)
            
        for i, r in dfFailed.iterrows():
            print ('Cleaning block: {}'.format(r['failed']))
            self.spCleanUpFailedBlock(r['failed'], alias, parseAgain)
        
    def spCleanUpFailedBlock(self, blockNumber, alias=None, parseAgain=True):
        self.seqldb.execute('exec spCleanUpByBlock {}'.format(blockNumber))
        if parseAgain:
            self.parseBlock(blockNumber, alias)
            
        
        
    def parseRange(self, start,  end, alias=None, getBalanceInfo=0, SAVE_TO_DB = True, printAtEnd = 0):            
        currentBlocks = self.getCurrentListOfBlocks()
        getBalanceInfo = 0
    
        startTime = datetime.datetime.now()
        for i in tqdm(range(start,	end)):
            i=int(i)
            if i in currentBlocks:
                continue
            try:
                self.parseBlock(i, alias, getBalanceInfo)
                currentBlocks.append(i)
            except Exception as ex:
                df = pd.DataFrame({'failed': [i]})
                df['message'] = str(ex)
                print ('Block {} - error: {}'.format(str(i), str(ex)))
                df.to_sql('failures', self.seqldb.seqlEngine, if_exists='append', index=False) if SAVE_TO_DB else None    
        print(datetime.datetime.now() - startTime)

    def parseAccountBlocks(self, acc, alias = '', getBalanceInfo = 0, transLimit = 99999999, updateAlias = 1):
        print ('Scanning', acc)
        AccAlias, nTrans  = esParser.getAccountAliasOnEtherScan(acc)

        if nTrans > transLimit:
            print ('Number of transactions ({}) greater than the limit'.format(nTrans))
            return
       
        blockList = esParser.getAccountBlocks(acc)           
        blockList.sort()

        if AccAlias == '':
            AccAlias = 'other'
        
        if updateAlias == 1:
            try:
                self.seqldb.execute("exec insertOrUpdateAccAlias '{}','{}'".format(acc, AccAlias))
            except:
                print ('Update alias procedure probably missing from the database. Try to set the updateAlias to 0')
                pass           
       
        for b in tqdm(blockList):
            b = int (b)
            #the block may already be in the DB 
            #method not ideal, but this shouldnt be a long procecss so its ok
            r = self.seqldb.execute('select top 1 * from block where number = {}'.format(b))
            if len(r) >0:
                continue
            try:
                self.parseBlock(b, alias, getBalanceInfo = 0)
            except Exception as ex:
                df = pd.DataFrame({'failed': [b]})
                df['message'] = str(ex)
                print ('Block {} - error: {}'.format(str(b), str(ex)))
                df.to_sql('failures', self.seqldb.seqlEngine, if_exists='append', index=False)
        

    