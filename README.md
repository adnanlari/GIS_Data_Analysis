# GIS_Data_Analysis
Data_Analysis(Sundarban)

## task.py

This is required to get connection_time related information between different nodes.

Format : Connected node, start_time, end_time,connection_time,size,data_rate

Input Files - psyncLog

Output Files - X_Connection_time.csv ( Node Name = X)

## task2.py

This is required to obtain collected message information of each node.

Format: timestamp, file_name,type, source,destination, deliverer ,size

Input File - PsyncLog

Output File - X_Message.csv

## slot.py

This is required to get data_rate.csv of a Node. 

Format: slot_no,connected node,data_rate,time

Input File - X_Connection_time.csv

Output File - data_rate.csv

## latest.py

This is required to get generated data information from each mobile node.

Format : timestamp, type, source, lat_long, file_name, size

Input File - latestKML file

Output File - X_info.csv

## KML/..../latestKML/slot_message_latest.py

getting slot and tile information of each generated data

Format: slot,tile_name,timestamp, type, source, lat_long, file_name, size

Input File : X_info.csv

Output File : slot_gen.csv

## slot_message.py 

getting slot information of collected data for a node.

Input File : X_Message.csv

Output File: slot_file.csv

## Real_Simulation/simulate.py

Input File : collected and generated slotted message files ( output of slot_message.py and slot_message_latest.py)
              So, input files are 'abc_c.csv' , 'abc_g.csv'

Output : Data for that corresponding node are inserted into InfluxDB
 
##  gen_to_db.py

Stores generated files information in Influxdb

Input File: 'abc_g.csv'

Output : information of generated files are stored into InfluxDB

## db_grid.py

Output : no of media files within each tile


