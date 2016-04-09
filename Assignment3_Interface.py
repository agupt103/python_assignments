#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import thread
import threading
import sys

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
##########################################################################################################



def sortpartition(i,tablename,columnname,outputTable,openconnection):
    cur = openconnection.cursor()
    cur.execute('INSERT INTO '+outputTable+str(i)+' SELECT * FROM '+ tablename +'range_part'+ str(i) + ' ORDER BY '+ columnname +';')
    openconnection.commit()
    cur.close()

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    cur = openconnection.cursor();
    cur.execute('DROP TABLE IF EXISTS '+OutputTable+';');
    cur.execute('CREATE TABLE IF NOT EXISTS '+OutputTable+' (like '+InputTable+');')
    print "Arpit"
    cur.execute('ALTER TABLE '+InputTable+' RENAME '+SortingColumnName+' TO '+InputTable+SortingColumnName+';')
    Sorting = SortingColumnName
    SortingColumnName = str(InputTable)+str(SortingColumnName);
    cur.execute('SELECT MAX('+ SortingColumnName +') FROM '+InputTable+';')
    max = cur.fetchone()[0]
    rangepartition(InputTable, SortingColumnName, max, openconnection)
    numberofpart = 5;

    for i in range(1,numberofpart+1):
        try:
            cur.execute('DROP TABLE IF EXISTS '+OutputTable+str(i)+';')
            cur.execute('CREATE TABLE IF NOT EXISTS '+OutputTable+str(i)+' ( like '+InputTable+' );')
            thread=threading.Thread(target=sortpartition(i,InputTable,SortingColumnName,OutputTable,openconnection))
            thread.start()
        except:
            print ('Something went wrong in ParallelSort')
    for i in range(1,numberofpart+1):
        cur.execute('INSERT INTO '+OutputTable+' SELECT * FROM '+OutputTable+str(i)+';')
    print ("Finish ParallelSort")
    print ("Check the Output Table ")
    cur.execute('ALTER TABLE '+InputTable+' RENAME '+SortingColumnName+' TO '+Sorting+';')
    openconnection.commit()
    cur.close()


   # pass #Remove this once you are done with implementation
def rangepartition(ratingstablename, columnname, max,openconnection):
        numberofpartitions = 5;
        open_con = openconnection
        open_cur = open_con.cursor()
        if numberofpartitions > 0 and isinstance(numberofpartitions, int):
         for var1 in range(1, numberofpartitions+1):
            open_cur.execute("DROP TABLE IF EXISTS "+ratingstablename+"range_part"+str(var1)+"")
            open_cur.execute("CREATE TABLE "+ratingstablename+"range_part"+str(var1)+" ( like "+ratingstablename+" );")
         open_con.commit()
         open_cur.execute("SELECT * FROM "+ratingstablename+" where "+columnname+" = 0")
         rows = open_cur.fetchall()
         #for r in rows:
          #open_cur.execute("INSERT INTO "+ratingstablename+"range_part1 VALUES("+str(r[0])+","+str(r[1])+","+str(r[2])+")")
         val = 0
         partition = float(5)/numberofpartitions
         for var2 in range(1, numberofpartitions+1):
            lower = partition*val
            upper = partition*(val+1)
            if var2 == 1:
                open_cur.execute("INSERT INTO "+ratingstablename+"range_part"+str(var2)+" SELECT * FROM "+ratingstablename+" WHERE "+columnname+" >= "+str(lower)+" AND "+columnname+" <= "+str(upper))
            elif var2 > 1:
                open_cur.execute("INSERT INTO "+ratingstablename+"range_part"+str(var2)+" SELECT * FROM "+ratingstablename+" WHERE "+columnname+" > "+str(lower)+" AND "+columnname+" <= "+str(upper))
            val = val+1
        # open_cur.execute("INSERT INTO TEMPORARYINFO VALUES('RANGE', %s, %s)",(str(var2+1),str(numberofpartitions)))
        open_con.commit()
        open_cur.close()

def joinpartition(i,InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn,OutputTable,openconnection):
    cur = openconnection.cursor()
    cur.execute('INSERT INTO '+OutputTable+' SELECT * FROM '+ InputTable1 +'range_part'+ str(i) + ' INNER JOIN '+InputTable2+'range_part'+ str(i)+' ON '+ InputTable1 +'range_part'+ str(i) +'.'+Table1JoinColumn+'='+ InputTable2 +'range_part'+ str(i) +'.'+Table2JoinColumn+';')
    openconnection.commit()
    cur.close()

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    numberofparts = 5;
    open_con = openconnection
    open_cur = open_con.cursor()
    OutputTable1 = str(OutputTable)+str(1);
    open_cur.execute('DROP TABLE IF EXISTS '+OutputTable+' ;')
    open_cur.execute('DROP TABLE IF EXISTS '+OutputTable1+' ;')
    open_cur.execute('ALTER TABLE '+InputTable1+' RENAME '+Table1JoinColumn+' TO '+InputTable1+Table1JoinColumn+';')
    open_cur.execute('ALTER TABLE '+InputTable2+' RENAME '+Table2JoinColumn+' TO '+InputTable2+Table2JoinColumn+';')
    print "Ar"
    open_cur.execute('CREATE TABLE ' + OutputTable1 +' AS SELECT * FROM '+InputTable1+ ' Inner join '+InputTable2+ ' ON ( '+InputTable1+'.'+InputTable1+Table1JoinColumn+'='+InputTable2+'.'+InputTable2+Table2JoinColumn +');')
    open_cur.execute("CREATE TABLE "+OutputTable+"( like "+OutputTable1+" );")
    Table1 = Table1JoinColumn;
    Table2 = Table1JoinColumn;
    Table1JoinColumn = str(InputTable1)+str(Table1JoinColumn);
    Table2JoinColumn = str(InputTable2)+str(Table2JoinColumn);
    open_cur.execute('SELECT MAX('+ Table1JoinColumn +') FROM '+InputTable1+';')
    maxtable1 = open_cur.fetchone()[0]

    open_cur.execute('SELECT MAX('+ Table2JoinColumn +') FROM '+InputTable2+';')
    maxtable2 = open_cur.fetchone()[0]

    if(maxtable1 == maxtable2):
        maxval = maxtable2
    elif (maxtable2 > maxtable1):
        maxval = maxtable2
    else:
        maxval = maxtable1

    rangepartition(InputTable1, Table1JoinColumn, maxval, openconnection)
    rangepartition(InputTable2, Table2JoinColumn, maxval, openconnection)

    for i in range(1,numberofparts+1):
        try:
            thread=threading.Thread(target=joinpartition(i,InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn,OutputTable,openconnection))
            thread.start()
        except:
            print ('Something went wrong in ParallelJoin')
    open_cur.execute('ALTER TABLE '+InputTable1+' RENAME '+Table1JoinColumn+' TO '+Table1+';')
    open_cur.execute('ALTER TABLE '+InputTable2+' RENAME '+Table2JoinColumn+' TO '+Table2+';')
    open_con.commit()
    open_cur.close()

    #pass # Remove this once you are done with implementation


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
        # Creating Database ddsassignment3
        print ("Creating Database named as ddsassignment3")
        createDB();

        # Getting connection to the database
        print ("Getting connection from the ddsassignment3 database")
        con = getOpenConnection();

        # Calling ParallelSort
        print ("Performing Parallel Sort")
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

        # Calling ParallelJoin
        print ("Performing Parallel Join")
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);

        # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
        saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
        saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

        # Deleting parallelSortOutputTable and parallelJoinOutputTable
        deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print ("Something bad has happened!!! This is the error ==> ", detail)
