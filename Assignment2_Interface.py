#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import string
from tempfile import mkstemp
from shutil import move
from os import remove, close

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #Implement RangeQuery Here.
    try:
     name = "RangeRatingsPart"
     name_Round = "RoundRobinRatingsPart"
     open_con = openconnection
     open_cur = open_con.cursor()
     row = open_cur.execute("select MAX(partitionnum) from RangeRatingsMetadata")
     (rows,) = open_cur.fetchone()
     i = 0
     abs_path = "RangeQueryOut.txt";
     f = open(abs_path,'w')
     if ratingMinValue <= ratingMaxValue:
      while i <= rows:
        a = str(i)
        newTableName = str(name + a)
        newTableName_Round = str(name_Round + a)
        print rows
        open_cur.execute("SELECT * FROM "+newTableName+" WHERE rating >= "+str(ratingMinValue)+" AND rating <="+ str(ratingMaxValue))
        data = open_cur.fetchall ()
        for row_data in data :
                f.write(newTableName)
                f.write(", ")
                f.write(str(row_data[0]))
                f.write(", ")
                f.write(str(row_data[1]))
                f.write(", ")
                f.write(str(row_data[2]))
                f.write("\n")
        open_cur.execute("SELECT * FROM "+newTableName_Round+" WHERE rating >= "+str(ratingMinValue)+" AND rating <=" + str(ratingMaxValue))
        data = open_cur.fetchall ()
        for row_data in data :
                f.write(newTableName_Round)
                f.write(", ")
                f.write(str(row_data[0]))
                f.write(", ")
                f.write(str(row_data[1]))
                f.write(", ")
                f.write(str(row_data[2]))
                f.write("\n")
        i+=1;
      f.close()
     else:
         print "Min is greater than Max"
     open_con.commit()
     open_cur.close()
    except Exception as detail:
        print(detail)

def PointQuery(ratingsTableName, ratingValue, openconnection):
   try:
     name = "RangeRatingsPart"
     name_Round = "RoundRobinRatingsPart"
     open_con = openconnection
     open_cur = open_con.cursor()
     row = open_cur.execute("select MAX(partitionnum) from RangeRatingsMetadata")
     (rows,) = open_cur.fetchone()
     rows = rows
     i = 0;
     abs_path = "PointQueryOut.txt";
     f = open(abs_path,'w')
     while i <= rows:
        a = str(i)
        newTableName = str(name + a)
        newTableName_Round = str(name_Round + a)
        open_cur.execute("SELECT * FROM "+newTableName+" WHERE rating = "+str(ratingValue))
        data = open_cur.fetchall ()
        for row_data in data :
                f.write(newTableName)
                f.write(", ")
                f.write(str(row_data[0]))
                f.write(", ")
                f.write(str(row_data[1]))
                f.write(", ")
                f.write(str(row_data[2]))
                f.write("\n")
        open_cur.execute("SELECT * FROM "+newTableName_Round+" WHERE rating = "+str(ratingValue))
        data = open_cur.fetchall ()
        for row_data in data :
                f.write(newTableName_Round)
                f.write(", ")
                f.write(str(row_data[0]))
                f.write(", ")
                f.write(str(row_data[1]))
                f.write(", ")
                f.write(str(row_data[2]))
                f.write("\n")
        i+=1;
     f.close()
     open_con.commit()
     open_cur.close()
   except Exception as detail:
    print(detail)
