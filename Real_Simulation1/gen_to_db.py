import csv
import datetime
import sys
import json
from dateutil import parser
import os
import time
import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np
import statistics
from pathlib import Path
from influxdb import SeriesHelper
from influxdb import InfluxDBClient

myclient = InfluxDBClient('127.0.0.1', 8086, 'root', 'root', database='simulation')

fld=[]
time_dict={}
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




f_list = os.listdir('./')
fld.append('slot_no')
fld.append('tile_name')
fld.append('time_stamp')
fld.append('source')
fld.append('file_name')
fld.append('size')
for j in range(len(f_list)):
    op_file =open(f_list[j],'r')
    f_name = os.path.basename(op_file.name)
    if f_name.endswith('_g.csv'):
        logread = csv.reader(op_file)
        if logread is not None:
            for row in logread:
                print (row)
                if len(row)!=1:
                    dc={}
                    dc['slot_no']=row[0]
                    dc['tile_name']=row[1]
                    dc['time_stamp']=str(row[2])
                    dc['source']=row[4]
                    dc['file_name']=row[6]
                    dc['size']=int(row[7])
                    query_st = "select file_name from gen1 where file_name= '" + row[6].strip() + "'"
                    res = myclient.query(query_st)
                    res = list(res.get_points(measurement='gen1'))
                    if len(res)==0:
                        MySeriesHelper(**dc)
                        print("file",row[6],'is entered')
                    else:
                        print("file exists")



q2 = 'select distinct(tile_name) from gen1'
re = myclient.query(q2)
re = list(re.get_points(measurement='gen1'))
for ind in range(len(re)):
    cur_tile = re[ind]['distinct']
    print("cur_tile = ",cur_tile)
    q3 = "select file_name from gen1 where tile_name='"+cur_tile+"'"
    rp = myclient.query(q3)
    rp = list(rp.get_points(measurement='gen1'))
    # print("Result ",rp)
    video=0
    audio=0
    image=0
    diff=0
    text =0
    mp=0
    for u in range(len(rp)):
        if rp[u]['file_name'].endswith('mp4'):
            video +=1
        elif rp[u]['file_name'].endswith('3gp'):
            audio +=1
        elif (rp[u]['file_name'].endswith('jpeg') or rp[u]['file_name'].endswith('jpg')):
            image +=1
        elif rp[u]['file_name'].endswith('diff'):
            diff +=1
        elif rp[u]['file_name'].endswith('png'):
            mp +=1
        else:
            text +=1
    tt=0
    f=open('gen.txt','a')
    f.write("cur_tile "+str(cur_tile)+"Video = "+str(video)+"Audio = "+str(audio)+"image = "+str(image)+"diff = "+str(diff)+"text = "+str(text)+"Map = "+str(mp)+"\n")
    f.close()
    print(cur_tile,":","Video = ",video,"Audio = ",audio,"image = ",image,"diff = ",diff,"text = ",text,"Map = ",mp)





