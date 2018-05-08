import sqlite3 as sq
import numpy as np

import matplotlib.pyplot as plt
plt.rc('font', size=12)
plt.rc('axes', labelsize=12)


""" Load the total counts of ngrams
    from the text file 'total_counts':
    http://storage.googleapis.com/books/ngrams/books/googlebooks-fre-all-totalcounts-20120701.txt
"""

def parseLineForTotalCount( line ):
    
    d = line.split(',')
    
    if len(d) != 4:
        d = None
    else:
        d = [ int(u) for u in d ]

    return d

# open the file
filepath = 'data/googlebooks-fre-all-totalcounts-20120701.txt'

with open(filepath) as textfile:  
    lines = textfile.readlines()
    
lines = lines[0]

lines = lines.split('\t')
data = [ parseLineForTotalCount(L) for L in lines ]
data = [ d for d in data if d ]

data = [ u for u in data if u[0] >= 1800 ] # filter before 1800

yearsTotal, total_match_count, total_page_count, total_volume_count = zip( *data )

yearsTotal, total_match_count, total_page_count, total_volume_count= [np.array(arr) for arr
                    in [yearsTotal, total_match_count, total_page_count, total_volume_count]]


""" Functions
"""
def openDB( databasePath= 'database.sqlite' ):
    dbconnection = sq.connect(databasePath)
    cursor = dbconnection.cursor()
    
    return cursor
  
    
def getCountPerYear( cursor, ngram, normed=True, k=None, volume=False, where='ngram' ):
    ''' Query the DB to obtain the timeserie
    
        return numpy Arrays
    '''
    countStr = 'volume_count' if volume else 'match_count'
    
    query = """ SELECT year, {countStr} from ngram 
            WHERE {where} IS ?
            ORDER BY year ASC """.format(countStr=countStr, where=where)
        
    
    total_count = total_volume_count if volume else total_match_count

    cursor.execute( query, (ngram, ) )

    countByYears = cursor.fetchall()
    
    #yearsLocal, matchesPerYear, VolumesPerYear = zip( *countByYears )

    matchesPerYear = groupBy( countByYears, yearsTotal, fillvalue=0 )
    
    matchesPerYear = np.array(matchesPerYear)
    years = np.array( yearsTotal )
    
    if normed:
        matchesPerYear = matchesPerYear / total_count
        
        #if volume:
        #    matchesPerYear = matchesPerYear * total_count.mean()
        
    # Moving average:
    if k==3:
        cs = matchesPerYear.cumsum()
        matchesPerYear = (cs[2:] - cs[:-2])/2
        years = years[1:-1]
    elif k:
        m = np.ones(k)
        matchesPerYear = np.convolve( matchesPerYear, m,  'same' )

    return years, matchesPerYear


def groupBy( XYdata, Xfinal, fillvalue=0 ):
    ''' Take a list of data point XYdata = [(x1, y1), (x2, y3),... ]
        and a list Xfinal of x-positions
        - Find the y_i corresponding to the x_i = xFinal_j
        - Pad with the `fillvalue` for the xFinal_j without corresponding x_i
    '''
    Xfinal = list(Xfinal)
    
    Ycomplete = [fillvalue]*len(Xfinal)
    for x, y in XYdata:
        
        if x in Xfinal:
            Ycomplete[Xfinal.index(x)] += y
        else:
            continue
    
    Ycomplete = np.array( Ycomplete )
    
    return Ycomplete




def plot(cursor, ngram, normed=True, k=None, volume=False ):
    ''' Perform the query and plot the timeserie 
    '''

    countStr = 'fraction' if normed else 'count'
    volumeStr = 'volume' if volume else 'match'
    titlestr = '%s of %s for `%s`' % (countStr, volumeStr, ngram)
        
    years, counts = getCountPerYear( cursor, ngram, normed=normed, k=k, volume=volume )

    plt.figure( figsize=(10, 3) )
    if not volume:
        plt.plot( years, 100*counts )
        plt.ylabel('%s of matches' % countStr  )
        plt.ylim( 0,  1.05*max(100*counts) )
    else:
        plt.plot( years, counts )
        plt.ylabel('%s of volumes' % countStr  )
        plt.ylim( 0,  1.05*max(counts) )
        
    plt.title( titlestr )
    plt.xlabel('years')
    plt.xlim( min(years),  max(years) )
    
    plt.axhline( y=counts.mean(), color='r', alpha=.5, linewidth=1 )