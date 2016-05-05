import csv
import time
import codecs

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
    testinglogFile = 'testinglogFile.txt'
    with open(testinglogFile, 'a') as log:
        log.write(now)

def writeLogFile(pqID, resultString):

    testinglogFile = 'testinglogFile.txt'

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

def checkVendorURLs():

    debug = input('Run with debug on? Enter "1" else any key\n')
    debug = str(debug)

    # get list of data from Aleph
    AlephURLList = readAlephList()

    # clean aleph list
    cleanAList = cleanAlephList(AlephURLList)

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

        pqURLResults = searchForURL(cleanAList, pqURL)
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

    print('done!')

    return resultLists

checkVendorURLs()