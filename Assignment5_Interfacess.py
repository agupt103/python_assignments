#!/usr/bin/python2.7
#
# Assignment3 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import math
import io
from math import degrees, radians
from decimal import *

def distance(latitude1,longitude1,latitude2,longitude2):
    Radius = 3959; #miles
    lat1=radians(latitude1)
    lat2=radians(latitude2)
    deltaLat=radians(latitude2-latitude1)
    deltaLong=radians(longitude2-longitude1)
    a = math.sin(deltaLat/2) * math.sin(deltaLat/2) + math.cos(lat1) * math.cos(lat2) * math.sin(deltaLong/2) * math.sin(deltaLong/2);
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a));
    d=Radius*c
    return d

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):

    cursor =collection.find({"city": str(cityToSearch).title()}) #{"city": "Tempe"}
    f = open(saveLocation1,'w')
    for doc in cursor:
        text = str(doc['name']).upper()+"$"+str(doc['full_address']).replace("\n"," ").upper()+"$"+str(doc['city']).upper()+"$"+str(doc['state']).upper()
        f.write(text+"\n")
    f.close()

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    cursor =collection.find({"categories": {"$in": categoriesToSearch}})
    latitude1=float(myLocation[0])
    longitude1=float(myLocation[1])
    f = open(saveLocation2, 'w')
    for doc in cursor:
        text = str(doc['latitude'])+";"+str(doc['longitude'])
        latitude2=float(doc['latitude'])
        longitude2=float(doc['longitude'])
        if maxDistance>=distance(latitude1,longitude1,latitude2,longitude2):
            text=str(doc['name']).upper()
            f.write(text+"\n")
    f.close()
