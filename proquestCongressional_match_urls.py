import csv
import time
import codecs
import sqlite3

#Name Log File
testinglogFile = 'testinglogFile.txt'

# read in the list of aleph bibs and URLs
def readAlephList():
    alephList = '856_like_proquest-congressional.csv'

    print('loading list... this might take a while...')

    AlephURLList = []
    with open(alephList, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            AlephURLList.append(row)

    print('done loading list...')

    return AlephURLList

# update the list of aleph bibs and urls from something raw to a series of tuples
def cleanAlephList(alephList):

    print('cleaning aleph list...')

    fauPrefix = 'http://ezproxy.fau.edu/login?url='

    # should be a list of tuples: (Bib, URL, FAUPrefix (logical))
    cleanAList = []
    for row in alephList:
        bib = row[1]
        url = row[4]
        url = url[url.find('$$u')+3:]
        url = url[:url.find("$")]

        fau = 0
        if fauPrefix in url:
            url = url.replace(fauPrefix, '')
            fau = 1

        cleanTup = (bib, url, fau)
        cleanAList.append(cleanTup)

    print('done cleaning aleph list')

    return cleanAList

# def createDB():
#     db = sqlite3.connect(':memory:')
#     db = sqlite3.connect('mydb')
#
#     cursor = db.cursor()
#     cursor.execute('''CREATE TABLE users(Bib TEXT, URL TEXT,FAU_URL INTEGER)''')
#     db.commit()
#
#     cursor.executemany(''' INSERT INTO users(Bib, URL, FAU_URL) VALUES(?,?,?)''', c)
#     db.commit()
#
#     db.close()


# idenifty which Aleph Bibs contain the URL being searched out
def searchForURL(cleanAList, findURL):
    # test url
    # findURL = 'http://congressional.proquest.com/congcomp/getdoc?CRDC-ID=CRS-1978-ECN-0018'

    testResult = []
    for row in cleanAList:
        result = ()
        if findURL in row[1]:
            result = (row[0], row[2])
            testResult.append(result)

    return testResult


# read in the list of vendor IDs and URLs

def readVendorList():
    vendorList = 'ProquestURLs.txt'

    print('loading vendor list... this might take a while...')

    vendorURLList = []
    with open(vendorList, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            vendorURLList.append(row)

    print('done loading vendor list...')

    return vendorURLList

def logStartEnd(now):
    with open(testinglogFile, 'a') as log:
        log.write('\n'+now)

def writeLogFile(pqID, resultString):

    # Removed to universal variable
    # testinglogFile = 'testinglogFile.txt'
    with codecs.open(testinglogFile, 'a', encoding='utf-8') as log:
        try:
            log.write('\n'+resultString)
        except UnicodeEncodeError:
            log.write('\n'+'failed to write record key: '+str(pqID)+'\n')

def bestBib(resultsList):

    countBibs = 0
    fauBib = []
    for r in resultsList:
        if r[1] == 1:
            countBibs += 1
            fauBib.append(r[0])

    return [countBibs, fauBib]

def writeCleanListToFile(cleanList):
    c = 'cleanList.txt'

    with open(c,'a') as x:
        for r in cleanList[1:100]:
            x.write(str(r[0])+'\t'+str(r[1])+'\t'+str(r[2])+'\n')

def updateDatabase(row, debug):

    debug = str(debug)

    db = sqlite3.connect('proquestUrlDB')

    cursor = db.cursor()

    bib = int(row[0])
    if debug == '1':
        print('bib is: ',str(bib))
    url = row[1]
    if debug == '1':
        print('url is: ', str(url))
    fau = int(row[2])

    cursor.execute('SELECT ID from PQURL where URL = ?', (url,))
    pqIDs = cursor.fetchall()
    if len(pqIDs) == 0:
        cursor.execute('insert into PQURL(URL, FAU) values(?, ?)', (url, fau,))
        if debug == '1':
            print('adding into PQURL')
        db.commit()

        cursor.execute('SELECT ID from PQURL where URL = ?', (url,))
        pqIDs = cursor.fetchall()

    foundBib = False
    for uID in pqIDs:
        testuID = uID[0]
        cursor.execute('SELECT Bib from Bib2URL where Bib = (?) and KeyPQURL in(?)', (bib, testuID,))
        pqBibs = cursor.fetchall()
        if len(pqBibs) > 0:
            foundBib = True
    if foundBib is False:
        usepqID = pqIDs[0][0]
        cursor.execute('insert into Bib2URL(Bib, keyPQURL) values(?, ?)', (int(bib), usepqID,))
        if debug == '1':
            print('adding into Bibs2URL')
        db.commit()

    db.close()


def checkVendorURLs():

    debug = input('Run with debug on? Enter "1" else any key\n')
    debug = str(debug)

    # get list of data from Aleph
    AlephURLList = readAlephList()

    # clean aleph list
    cleanAList = cleanAlephList(AlephURLList)

    # create SQLite DB
    print('Building SQLiteDB...')

    # db = sqlite3.connect(':memory:')
    db = sqlite3.connect('proquestUrlDB')

    cursor = db.cursor()

    try:
        cursor.execute('''CREATE TABLE PQURL (ID integer primary key not null, URL TEXT UNIQUE, FAU Integer);''')
        db.commit()
    except sqlite3.OperationalError:
        if debug == '1':
            print('table PQURL already exists, skipping')

    try:
        cursor.execute('''CREATE TABLE Bib2URL (BibID integer primary key not null, Bib integer, keyPQURL integer, FOREIGN KEY(keyPQURL) references PQURL(id));''')
        db.commit()
    except sqlite3.OperationalError:
        if debug == '1':
            print('table Bib2URL already exists, skipping')

    # index the URLs
    try:
        cursor.execute(''' CREATE INDEX url_index on Bib2URL (Bib, KeyPQURL) ''')
    except sqlite3.OperationalError:
        if debug == '1':
            print('index url_index already exists, skipping')

    print('indexing done')

    print('Finished Building SQLiteDB')

    db.close()

    print('building database of URLs...')
    for row in cleanAList:
        try:
            int(row[0])
            updateDatabase(row, debug)
        except ValueError:
            if debug == '1':
                print('skipping because cannot convert bib to Int - ',row[0])


    print('finished writing database...')

    if debug == 1:
        stopper = input('press "n" to break at this point')
        if stopper == 'n':
            print(100/0)

    # load vendor list
    vendorList = readVendorList()

    now = time.strftime('%Y-%m-%d %H:%M:%S')
    now = 'starting at: ' + str(now)
    logStartEnd(now)

    print('running comparisons...')
    resultLists = {}


    for url in vendorList:

        resultString = ''
        pqURLResults = ''

        if debug == '1':
            print ('checking for url: ')

        pqID = url[0]
        resultString = resultString+str(pqID)

        if debug == '1':
            print('\t'+str(pqID))
        pqURL = url[3]
        resultString = resultString + '\t'+ str(pqURL)

        if debug == '1':
            print('\t' + str(pqURL))

        # find matches for the URL
        cursor.execute('''SELECT bib, fau_url FROM users WHERE URL=?''', (pqURL,))
        pqURLResults = cursor.fetchall()


        # pqURLResults = searchForURL(cleanAList, pqURL)
        resultString = resultString + '\t' + str(pqURLResults)

        if debug == '1':
            print('\t found bibs: '+str(pqURLResults))

        # find the best bib found, if any

        fauBibsFound = bestBib(pqURLResults)
        bibCount = fauBibsFound[0]

        fauBibs = ''
        if bibCount > 0:
            if bibCount > 1:
                for b in fauBibsFound[1]:
                    fauBibs = fauBibs+str(b)+', '
            else:
                fauBibs = str(fauBibsFound[1][0])

        # add to results logs
        resultString = resultString+'\t'+str(bibCount)+'\t'+str(fauBibs)

        if debug == '1':
            if bibCount > 0:
                print('\t fau bibs identified: ' + str(bibCount)+': '+str(fauBibs))
            else:
                print('\t no fau bibs found')


        # write to results string
        writeLogFile(pqID, resultString)

        resultLists[pqURL] = pqURLResults

        if debug == '1':
            print('length of results list is now: '+str(len(resultLists))+'\n')


    now = time.strftime('%Y-%m-%d %H:%M:%S')
    now = 'finished at: ' + str(now)
    logStartEnd(now)

    cursor = db.cursor()
    cursor.execute('''DROP TABLE ProquestURLs''')
    db.commit()

    db.close()
    print('done!')

    return resultLists

checkVendorURLs()