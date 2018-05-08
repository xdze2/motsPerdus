import gzip
import sqlite3 as sq
import os.path
import subprocess

# # Populate the Database
# 
# - Read the '.gz' archive file [downloaded here](http://storage.googleapis.com/books/ngrams/books/datasetsv2.html)
# - Fill a table with the informations: ngram, year, match_count, volume_count, bulk, cat
# 
# `bulk` est le ngram sans la categorie grammatical, et en minuscule.
# `cat` est la categorie grammatical.



letter =  input('letter? ')  


filepath = 'data/'+'googlebooks-fre-all-1gram-20120701-%s.gz' % letter

if not os.path.isfile(filepath):
    raise NameError('no data file')
else:
    print( filepath )


databasefilename = 'data/database-%s.sqlite' % letter


dbconnection = sq.connect('data/database-%s.sqlite'%letter)
cursor = dbconnection.cursor()


cursor.execute( '''DROP TABLE IF EXISTS ngram''' )

createTable = """ CREATE TABLE ngram (
                    id INTEGER PRIMARY KEY,
                    ngram TEXT,
                    year INTEGER,
                    match_count INTEGER,
                    volume_count INTEGER,
                    bulk TEXT,
                    cat TEXT)
                """

cursor.execute( createTable )



def parseLine( line ):
    ''' from the google doc:
        ngram TAB year TAB match_count TAB volume_count NEWLINE
    '''
    L = line.split('\t')
    
    ngram = L[0]
    ngram_splited = ngram.split('_')
    if len(ngram_splited)==2:
        bulk = ngram_splited[0].lower()
        cat = ngram_splited[1]
    else:
        bulk = ngram.lower()
        cat = None
    
    dico = {'ngram':ngram,
            'year':int(L[1]),
            'match_count':int(L[2]),
            'volume_count':int(L[3]),
            'bulk':bulk,
            'cat':cat}
    
    return dico


def insertManylines( cursor, entries ):
    insertquery = '''INSERT INTO ngram (
                     ngram, year, match_count, volume_count, bulk, cat )
                     VALUES(?, ?, ?, ?, ?, ?) '''

    cursor.executemany(insertquery, entries)






# Command to get the number of lines:
command = 'zcat %s | wc -l' % filepath
Ntot = subprocess.check_output(command, shell=True)
Ntot = int(Ntot)
print(Ntot, ' lines')
print('%e'%Ntot)

howmany = input('> Howmany lines ? (full, 10k) ')

if howmany == '10k':
    Nmax = 10000
    bufferSize = 500
elif howmany == 'full':
    Nmax = Ntot
    bufferSize = 50000
else:
    print( 'exit' )
    quit()



buffer = []

with gzip.open(filepath, 'rt') as textfile:  
    
    for k in range( Nmax ):
        line = textfile.readline()
        d = parseLine(line)
        
        if d['year'] < 1800:
            continue
            
        entry = [d['ngram'], d['year'], d['match_count'], d['volume_count'], 
                 d['bulk'], d['cat']]
        buffer.append( entry )
        
        if k % bufferSize == 0:
            insertManylines( cursor, buffer )
            buffer = []
            percent = k/Ntot*100
            print( '\r %.3f %%'%percent, end='' )
        
dbconnection.commit()
print('done')


# In[13]:

print( 'creat index for ngram' )
createIndex = "CREATE INDEX indexNgram ON ngram (ngram);"
cursor.execute(createIndex)


# In[14]:

print( 'creat index for bulk' )
createIndex = "CREATE INDEX indexBulk ON ngram (bulk);"
cursor.execute(createIndex)


''' Create the table `countbulk`
    from the count on the `bulk`
'''
print('\n Build the table `countbulk` using the bulk:')

cursor.execute( '''DROP TABLE IF EXISTS countbulk''' )

createCountTable = """CREATE TABLE countbulk AS
                      SELECT bulk, sum(match_count) AS matchcount, 
                             sum(volume_count) AS volumecount, 
                             GROUP_CONCAT(DISTINCT ngram) AS ngrams
                      FROM ngram
                      GROUP BY bulk """

cursor.execute( createCountTable )

dbconnection.commit()


# Get the top 10:
getTop10 = """SELECT * from countbulk
                      ORDER BY matchcount DESC
                      LIMIT 10"""

cursor.execute( getTop10 )

rows = cursor.fetchall()
for row in rows:
    print(row)



# Build the table `countngram` using the ngram:
print('\n Build the table `countngram` using the ngram:')
cursor.execute( '''DROP TABLE IF EXISTS countngram''' )

createCountTable = """CREATE TABLE countngram AS
                      SELECT ngram, sum(match_count) AS matchcount,
                      sum(volume_count) AS volumecount
                      FROM ngram
                      GROUP BY ngram """

cursor.execute( createCountTable )

dbconnection.commit()

# Get the top 30:
getTop30 = """SELECT ngram, matchcount, volumecount from countngram
                      ORDER BY matchcount DESC
                      LIMIT 10"""

cursor.execute( getTop30 )

rows = cursor.fetchall()
for row in rows:
    print( '%s %i matches  %i volumes ' % row)
    
  


cursor.close()
print('\n DB closed')
