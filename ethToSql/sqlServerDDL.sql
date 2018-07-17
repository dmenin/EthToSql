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


create view vwBlock as
SELECT [number]
      ,[transCount]
      ,[uniqueAccounts]
      ,[contractCount]
      ,[hash]
      ,[parentHash]
      ,[miner]
      ,[nonce]
      ,[timestamp]
      ,[difficulty]
      ,[totalDifficulty]
      ,[gasLimit]
      ,[gasUsed]
      ,[receiptsRoot]
      ,[stateRoot]
      ,[transactionsRoot]
      ,[sha3Uncles]
      ,[size]
FROM [dbo].[Block];

alter view vwBlockTransaction as
SELECT [blockNumber]
      ,[transactionIndex]
      ,[hash]
      ,[from] as fromAcc
	  ,isnull(a1.alias , 'other') as fromAlias
      ,[to]   as toAcc
      ,isnull(a2.alias , 'other') as toAlias
	  ,[contractCreated]
      ,[valEth]
      ,[valFinney]
      ,[valSzabo]
      ,[value]
      ,[gas]
      ,[gasPrice]
      ,[nonce]
  FROM [dbo].[BlockTransaction] b
	   join accountAlias a1 on a1.account = b.[from]
	   join accountAlias a2 on a2.account = b.[to];
 

create view vwContract as
SELECT [blockNumber]
      ,[transactionHash]
      ,[contractAddress]
      ,[creator]
      ,[gasUsed]
      ,[transactionIndex]
      ,[cumulativeGasUsed]
  FROM [dbo].[Contract];



alter view vwAccountBalances
as
SELECT [blockNumber], account, [balEth], [balFinney], [balSzabo], [balance]
FROM [dbo].[AccountBalances];

create view vwAccountAlias
as
select account, alias
from AccountAlias 