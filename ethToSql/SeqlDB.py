import logging
from sqlalchemy import create_engine
import pandas as pd
    
LOG = logging.getLogger(__name__)

class SeqlDB(object):

    
    def __init__(self, conString, dbVendor = 'SqlServer'):  
        self.dbVendor   = dbVendor
        self.conString  = conString
        self.seqlEngine = self.createEngine()
        self.setUpIfNotExist()

    def createEngine(self):          
        engine = create_engine(str(self.conString))
        return engine
    
    def getConnection(self):    
        engine =  self.seqlEngine
        LOG.info('Connecting to the database')
        connection = engine.connect()
        return connection
        
    def execute(self, sql):        
        with self.getConnection() as db_connection:
            trans = db_connection.begin()
            try:
                result = db_connection.execute(sql)
                if result.returns_rows:
                    cNames = result.keys()
                    data = pd.DataFrame(result.fetchall(), columns=cNames)
                else:
                    data = None
                
                trans.commit()
            except Exception as ex:
                trans.rollback()
                raise ex
            return data
    
    def setUpIfNotExist(self):
        if self.dbVendor == 'SqlServer':
            exists = self.execute('''
                        SELECT * 
                        FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_SCHEMA = 'dbo' 
                        AND  TABLE_NAME = 'Block'
                    ''')
            if len(exists) == 0:
                self.execute('''
                    CREATE TABLE [dbo].[Block](
                    	[number] [int] NOT NULL,
                    	[transCount] [int] NULL,
                    	[uniqueAccounts] [int] NULL,
                    	[contractCount] [int] NULL,
                    	[hash] varchar(66) NULL,
                    	[parentHash] varchar(66) NULL,
                    	[miner] varchar(66) NULL,
                    	[nonce] varchar(18) NULL,
                    	[timestamp] [datetime] NULL,
                    	[difficulty] [bigint] NULL,
                    	[totalDifficulty] [bigint] NULL,
                    	[gasLimit] [bigint] NULL,
                    	[gasUsed] [bigint] NULL,
                    	[receiptsRoot] varchar(66) NULL,
                    	[stateRoot] varchar(66) NULL,
                    	[transactionsRoot] varchar(66) NULL,
                    	[sha3Uncles] varchar(66) NULL,
                    	[size] [bigint] NULL,
                    	[alias] varchar(100),
                    	[hasAccountBalanceInfo] int,
                    	PRIMARY KEY CLUSTERED([number])
                    );
                    
                    CREATE TABLE [dbo].[BlockTransaction](
                    	[blockNumber] int not null,
                    	[transactionIndex] int not null,
                    	[hash] varchar(66),
                    	[from] varchar(42),
                    	[to] varchar(42),
                    	[contractCreated] varchar(42),
                    	[valEth] [float] NULL,
                    	[valFinney] [float] NULL,
                    	[valSzabo] [float] NULL,
                    	[value] [float] NULL,
                    	[gas] [bigint] NULL,
                    	[gasPrice] [bigint] NULL,
                    	[nonce] [bigint] NULL,
                    	primary key([blockNumber], [transactionIndex])
                    );
                    
                    CREATE TABLE [dbo].[Contract](
                    	[blockNumber] int not null,
                    	[transactionHash] varchar(66),
                    	[contractAddress] varchar(42),
                    	[creator] varchar(42),
                    	[gasUsed] bigint,
                    	[transactionIndex] int ,
                    	[cumulativeGasUsed] bigint
                    );
                    
                    create table AccountAlias(
                    	[account] varchar(42),
                    	[alias]   varchar(100),
                    	primary key (account)
                    );
                    
                    CREATE TABLE [dbo].[AccountBalances](
                    	[blockNumber] int not null,
                    	[account] varchar(42),
                    	[balEth] [float],
                    	[balFinney] [float],
                    	[balSzabo] [float],
                    	[balance] [float],
                    	primary key (blockNumber, account)
                    );

                    CREATE TABLE [dbo].[failures](
                    	[failed] [bigint] NULL,
                    	[message] [varchar](max) NULL
                    );
                    ''')
                self.execute('''
                    create procedure spCleanUpByBlock(@blockNumber int) as
                    begin
                    	delete from [dbo].[AccountBalances] where blockNumber = @blockNumber
                    	delete from [dbo].[Block] where number = @blockNumber
                    	delete from [dbo].[Contract] where blockNumber = @blockNumber
                    	delete from [dbo].[BlockTransaction] where blockNumber = @blockNumber
                    	delete from failures where failed = @blockNumber
                    end;
                ''')
                
                #Used on the ParseBlockFunction
                self.execute('''
                    create procedure insertIfNotExistAccAlias(@account varchar(42), @alias varchar(100))
                    as
                    begin
                    	IF NOT EXISTS(SELECT * FROM AccountAlias WHERE account = @account) begin
                    		insert into AccountAlias values (@account, @alias)
                    	end 
                    end;
                ''')
                
                #TODO: this second procedure could replace the first completelly
                #used on the parseAccount procedure
                self.execute('''
                    create procedure insertOrUpdateAccAlias(@account varchar(42), @alias varchar(100))
                    as
                    begin
                    	IF NOT EXISTS(SELECT * FROM AccountAlias WHERE account = @account) begin
                    		insert into AccountAlias values (@account, @alias)
                    	end ELSE begin
                    		update AccountAlias set alias = @alias where account = @account and (alias = 'other' or alias is null)
                    	end
                    end
                ''')                     