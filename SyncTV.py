import sys,httplib,re
import logging
from datetime import date
import sqlite3
import os

class dbi:
    def __init__(self):
        #connect to sqlite db
        self.connection = sqlite3.connect('TVseries.db')
        self.cur = self.connection.cursor()

        #create table series for fresh initialization
        try:
            self.cur.execute("SELECT * from series")
            #__dbseries = self.cur.fetchone()
        except sqlite3.OperationalError:
            self.cur.execute('''CREATE TABLE series (id INTEGER PRIMARY \
                             KEY AUTOINCREMENT, name TEXT, \
                             episodes INTEGER, url TEXT)''')
            self.connection.commit()

    def dbTableCreate(self, nameseries):
        sqlstring =  'CREATE TABLE ' + nameseries + \
            ' (id INTEGER PRIMARY KEY AUTOINCREMENT, episode INTEGER, \
            url TEXT, downloaded INTEGER, watched INTEGER)'
        self.cur.execute(sqlstring)
        self.connection.commit()

    def dbMainTableInsert(self, name, episodes, url):
        sqlstring = 'INSERT INTO series (name, episodes, url) VALUES(\'' + \
            name + '\', ' + str(episodes) + ', \'' + url + '\')'
        self.cur.execute(sqlstring)
        self.connection.commit()

    def dbSubTableInsert(self, nametable, episode, url, downloaded, watched):
        sqlstring = 'INSERT INTO ' + nametable + \
            ' (episode, url, downloaded,  watched)  VALUES(' \
            + str(episode) + ', \'' + url + '\', ' + str(downloaded) + \
            ', ' + str(watched) + ')'
        self.cur.execute(sqlstring)
        self.connection.commit()

    def dbTableDrop(self, nameseries):
        sqlstring = 'DROP TABLE ' + nameseries
        self.cur.execute(sqlstring)
        self.connection.commit()

    def dbMainTableUpdate(self, name, episodes):
        self.cur.execute("UPDATE series SET episodes=(?) WHERE name=(?)",\
                         (episodes,name))
        self.connection.commit()

    def dbSubTableUpdate(self, nametable, episode, downloadset, watchset):
        if (downloadset == 1):
            sqlstring = 'UPDATE ' + nametable + ' SET downloaded=1 WHERE episode=' + str(episode)
            self.cur.execute(sqlstring)

        if (watchset == 1):
            sqlstring = 'UPDATE ' + nametable + ' SET watched=1 WHERE episode=' + str(episode)
            self.cur.execute(sqlstring)

            self.connection.commit()

    def dbSeriesQuery(self):
        self.cur.execute('SELECT name FROM series')
        return self.cur.fetchall()

    def dbEpisodesQuery(self,series):
        self.cur.execute('SELECT episodes FROM series WHERE name=?',(series,))
        return self.cur.fetchall()

    def dbUrlQuery(self,series):
        self.cur.execute('SELECT url FROM series WHERE name=?',(series,))
        return self.cur.fetchall()

    def dbSubTableQuery(self,series):
        sqlstring = 'SELECT * FROM \'' + series + '\''
        self.cur.execute(sqlstring)
        print self.cur.fetchall()

    def dbClose(self):
        self.connection.close()

def reqHTTP(tvUrl):
    logWrapper("Start working on [url="+tvUrl+"]","info")
    conn = httplib.HTTPConnection('www.meijutt.com')
    conn.request('GET',tvUrl)
    res = conn.getresponse()
    logWrapper("HTTP response received and reads [" + str(res.status) + "]","info")
    return res.read()

def parseSeries(tvUrl):
    content = reqHTTP(tvUrl)
    series = re.findall('ed2k://.+?/',content)
    return series

def logWrapper(msg,level):
    logDate = date.today().isoformat()
    logging.basicConfig(filename='Log/'+logDate,level=logging.DEBUG,format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

    if (level == "warn"):
        logging.warning(msg)
    elif(level == "debug"):
        logging.debug(msg)
    elif(level == "info"):
        logging.info(msg)
    elif(level == "error"):
        logging.error(msg)
    else:
        print "==in logWrapper()==,wrong args"

#main logic
def seriesUpdate(db):
    '''read current episodes from main table
    where downloading begins'''
    series = db.dbSeriesQuery()
    for s in series:
        currep = db.dbEpisodesQuery(s[0])[0][0]
        try:
            db.dbTableCreate(s[0])
        except sqlite3.OperationalError:
            pass

        #send request to content server, parse reponse
        url = db.dbUrlQuery(s[0])[0][0]

        ed2klist = parseSeries(url)

        for l in ed2klist[currep:]:
            db.dbSubTableInsert(s[0],ed2klist.index(l)+1,l,0,0)
            callcmd = 'amulecmd -h localhost -p 4712 -c \'Add ' + l + ' \''
            os.system(callcmd)

        #db.dbSubTableQuery(s[0])

if __name__ == "__main__":
   #initialize database instance
    db = dbi()

    #series along with url are inserted to main table
    #via DB interface
    db.dbMainTableInsert('Blacklist',9,'/content/meiju21471.html')
    #db.dbMainTableInsert('Homeland5',0,'')

    seriesUpdate(db)

    db.dbClose()
