import requests
from bs4 import BeautifulSoup
import time
URL = 'https://etherscan.io/txs?a={}&p={}'

def getDataFromURL(sess, page, ACCOUNT):
    url = URL.format(ACCOUNT, page)
    print("Retrieving page", page)
    return BeautifulSoup(sess.get(url).text, 'html.parser')

def getPage(sess, page, ACCOUNT):    
    soup = getDataFromURL(sess, str(int(page)), ACCOUNT) # returns the page i
    mainBody = soup.findAll('tbody')[0] #gets the table body
    
    blockList = [] 
    for row in mainBody.findAll('tr'):
        columns = row.findAll('td')
        if len(columns) == 0:
            return None
        blockNumber = columns[1].contents
        number = blockNumber[0].getText()
        blockList.append(number)

    #remove duplicates
    blockList = list(set(blockList))
    return blockList

'''
Parses EtherScan and gets a list of blocks 
where there are transactions for an account
'''
def getAccountBlocks(ACCOUNT):
    sess = requests.Session()
    
    FinalBlockList = []
    page = 0
    while True:
        time.sleep(1) #to avoid timeouts
        page += 1
        result = getPage(sess, page, ACCOUNT)
        if result != None:
            FinalBlockList.extend(result)
        else:
            break
            
    FinalBlockList = list(set(FinalBlockList))
    return FinalBlockList


def getDataFromAccountURL(sess, URL, ACCOUNT):
    url = URL.format(ACCOUNT)
    print("Looking at acc", ACCOUNT)
    return BeautifulSoup(sess.get(url).text, 'html.parser')



def getAccountAliasOnEtherScan(Account):
    AccURL = 'https://etherscan.io/address/{}'
    sess = requests.Session()
    soup = getDataFromAccountURL(sess, AccURL, Account)
    alias = '' 
    try:
        soup.find('font')['title']
        alias = soup.find('font').getText()
    except:
        pass
    
    #<span data-placement="bottom" rel="tooltip" title="Normal Transactions">2317 txns </span>
    s = soup.findAll("span", {"title" : "Normal Transactions"})
    ntrans = 0
    try:
        ntrans = int(s[0].text.replace('txns',''))
    except:
        pass

    return alias, ntrans



