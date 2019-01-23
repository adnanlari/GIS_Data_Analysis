import csv
import datetime
import sys
import json
from dateutil import parser
import os
import time
# import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np
import statistics
from pathlib import Path
from influxdb import SeriesHelper
from influxdb import InfluxDBClient

myclient = InfluxDBClient('127.0.0.1', 8086, 'root', 'root', database='simulation')

fld=[]
time_dict={}
file_dict={}
class MySeriesHelper(SeriesHelper):
    # """Instantiate SeriesHelper to write points to the backend."""

    class Meta:
        # """Meta class stores time series helper configuration."""

        # The client should be an instance of InfluxDBClient.
        client = myclient

        # The series name must be a string. Add dependent fields/tags
        # in curly brackets.
        series_name = 'gen1'

        # Defines all the fields in this time series.
        fields = fld

        # Defines all the tags for the series.
        tags = []

        # Defines the number of data points to store prior to writing
        # on the wire.
        bulk_size = 0

        autocommit = True

q2 = 'select m_id from sDB1'
req = myclient.query(q2)
req = list(req.get_points(measurement='sDB1'))

for j in range(len(req)):
    cur_file = req[j]['m_id'].strip()
    # print("cur_file ",cur_file)
    q3 = "select tile_name from gen1 where file_name='"+cur_file+"'"
    rt = myclient.query(q3)
    rt = list(rt.get_points(measurement='gen1'))
    if len(rt)!=0:
        cur_tile = rt[0]['tile_name']
        ll=[0]*6
        if cur_tile not in file_dict:
            file_dict[cur_tile]=ll
        else:
            ll=file_dict[cur_tile]
        if cur_file.endswith('mp4'):
            ll[0]+=1
        elif cur_file.endswith('3gp'):
            ll[1]+=1
        elif cur_file.endswith('png'):
            ll[2]+=1
        elif (cur_file.endswith('jpeg') or cur_file.endswith('jpg')):
            ll[3]+=1
        elif (cur_file.endswith('diff')):
            ll[4]+=1
        else:
            ll[5]+=1
        file_dict[cur_tile]=ll

    else:
        print("no tile for ",cur_file)


for k,v in file_dict.items():
    print("Tile_name : ",k)
    print("Video= ",v[0],"Audio = ",v[1],"map= ",v[2],"image = ",v[3],"Diff = ",v[4],"Text = ",v[5])
    f=open('db.txt','a')
    f.write("Tile_name :"+str(k)+"Video= "+str(v[0])+"Audio = "+str(v[1])+"map= "+str(v[2])+"image = "+str(v[3])+"Diff = "+str(v[4])+"Text = "+str(v[5])+"\n")
    f.close()

