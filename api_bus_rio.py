import os
import subprocess
import psycopg2
import psycopg2.extras
import pprint
import time
import datetime as dt
import pytz
from datetime import datetime
import sys
#---------
reload(sys)
sys.setdefaultencoding('utf-8')
#---------
import signal
import subprocess
#----------------------
import urllib2
#import urllib
import hashlib
import json
#-----------------------------------------------
HOST = 'localhost'
DB_NAME = 'xmiles'
USER = 'power_user'
PASS = 'power_user'
#-----------------------------------------------

#SLEEP_TIME = 10 # 10 secs.
SLEEP_TIME = 40 # 40 secs

#DELTA_HOUR = 03 # 03 hours.
DELTA_HOUR = 02 # 02 hours.
#-----------------------------------------------
BUS_API_TIME_STAMP = 0
BUS_API_BUSCODE    = 1
BUS_API_BUSLINE	   = 2
BUS_API_LATITUDE   = 3
BUS_API_LONGITUDE  = 4
#-----------------------------------------------
#DEBUG = True
DEBUG = False
#-----------------------------------------------


def execute_query_db(query):
        conn_string = "host=%s dbname=%s user=%s password=%s" % (HOST, DB_NAME, USER, PASS)

        #print "Connecting to database\n ->%s" % (conn_string)

        conn = psycopg2.connect(conn_string)
        #conn.commit()

        cursor = conn.cursor('cursor_find_files', cursor_factory=psycopg2.extras.DictCursor)

        print "Executing query: %s" % (query)

        cursor.execute(query)
        records = cursor.fetchall()
        #print records
        conn.commit()
        return records

def insert_table(query):
        conn_string = "host=%s dbname=%s user=%s password=%s" % (HOST, DB_NAME, USER, PASS)

        conn = psycopg2.connect(conn_string)

        cursor = conn.cursor()
	if DEBUG:
        	out_log.write("\n")
        	#print "Executing query: %s\n" % (query)
        	out_log.write("Executing query: %s\n" % (query))

	print "Executing query: %s\n" % (query)

        cursor.execute(query)

        conn.commit()
        print "Total number of rows updated: %s\n" % (cursor.rowcount)
	if DEBUG:
        	out_log.write("Total number of rows updated: %s\n" % (cursor.rowcount))


def mkdir(folder):
    return os.system('mkdir "%s"' % folder)



def sigint_handler(signal, frame):
        print "\n\nYou pressed Ctrl+C!, stopping script"
        print time.ctime()
        print "Exiting"
        sys.exit(0)

#Binding SIGINT to a handler, so we can gracefully stop this script
signal.signal(signal.SIGINT, sigint_handler)

#
# SCRIPT FOR PARSING FILES
#
#---------------------------------
CollectionDate =''.join(str(datetime.now())[0:10].split("-"))
folder = "/usr/xmiles/newsfeed_update/logs/" +  CollectionDate + "_log"
out_file = folder + "/apibusrio_out.log2"

if not os.path.isdir(folder):
  mkdir(folder)

out_log = open(out_file,'w')
#---------------------------------

print "Starting xmiles ApiBusRio"
print time.ctime()


while True:

        CollectionDate =''.join(str(datetime.now())[0:10].split("-"))
        folder = "/usr/xmiles/newsfeed_update/logs/" +  CollectionDate + "_log"
        out_file = folder + "/apibusrio_out.log2"

        if not os.path.isdir(folder):
          mkdir(folder)
          out_log = open(out_file,'w')

        out_log.close()
        out_log = open(out_file,'a')

        msg = "-------------------\nRun PostgreSQL Func at %s\n-------------------" % time.ctime()
        print msg
	if DEBUG:
        	out_log.write(msg)

 	#-------------------------
        query_2 = "select max(time_stamp) from api.bus_rio";
	#query_1 = "select exists(" + query_2 + ")";
	#ts_checkout = execute_query_db(query_1)[0][0]

	ts_checkout = True
	#ts_checkout = False

	print ts_checkout
	if ts_checkout:
        	max_ts = execute_query_db(query_2)[0][0]
		print max_ts
		
	else:
		max_ts = dt.datetime(2014,8,8,15,00)
		print max_ts
        #-------------------------
	

        try:
                
		url = "http://webapibrt.rio.rj.gov.br/api/v1/brt"
		"""
		response = urllib2.urlopen(url)
		array = json.loads(response.read())
		data = array['veiculos']
		
		tz = pytz.timezone('America/Sao_Paulo')
						
		#-------------------------
		query = "insert into api.bus_rio (buscode,busline,bus_type,latitude,longitude,hashcode,time_stamp) VALUES"

		flag_action = False

		for i in range(len(data)):
		#for i in range(2):        
		    s = data[i].get('datahora')/1000.0
		    
		    #stime = dt.datetime.fromtimestamp(s)
		    stime_tz = dt.datetime.fromtimestamp(s, tz)
		    stime = stime_tz.replace(tzinfo=None)	    
		    #----------------
		    #print stime
		    
		    #---------------
                    delta = max_ts - stime
	    	    #delta = dt.timedelta(0, 3600)
		    #print delta
		    #if delta < dt.timedelta(0, 3600):
                    #if delta < dt.timedelta(0, 10800):
		    if delta < dt.timedelta(0, 7200):

                        if flag_action:
                            query = query + ","
                            
                        flag_action = True
    			#print flag_action	    
		
		    	query = query + " (%r,%r,%r,%f,%f,%r,%r)" % (str(data[i].get('codigo')), str(data[i].get('linha')),'BRT',data[i].get('latitude'), data[i].get('longitude'),str(hashlib.md5(str(data[i].get('datahora'))+str(data[i].get('codigo'))).hexdigest()),str(dt.datetime.fromtimestamp(s, tz)))

		    	 
                    #else:
		    #	print "stime: " + str(stime)
		    #	print "delta: " + str(delta) 
		    #	print "flag_action: " + str(flag_action)
		    #	break
 
		if flag_action:    
			insert_table(query)

		time.sleep(SLEEP_TIME)

		"""				


        except SystemExit:
                break
        except:
                error = "Error BRT API"
                print time.ctime()
                print error

 		time.sleep(SLEEP_TIME)
                
                print str(sys.exc_info())
		if DEBUG:
                	out_log.write(error)
                	out_log.write(str(sys.exc_info()[0]))

                continue


        try:
                              
		
		url = "http://dadosabertos.rio.rj.gov.br/apiTransporte/apresentacao/rest/index.cfm/onibus"
		#"""
		request = urllib2.Request(url, headers={"Accept" : "application/json"})
		response = urllib2.urlopen(request)

		array = json.loads(response.read())
		
		data = array['DATA']
	        	
		
                query = "insert into api.bus_rio (buscode,busline,bus_type,latitude,longitude,hashcode,time_stamp) VALUES"

		flag_action = False

                for i in range(len(data)):

	       	    stime = []
                    stime.append(data[i][BUS_API_TIME_STAMP][0:10].split("-")[2])
                    stime.append(data[i][BUS_API_TIME_STAMP][0:10].split("-")[0])
                    stime.append(data[i][BUS_API_TIME_STAMP][0:10].split("-")[1])
	       	   
                    #-----------------------
		    str_stime = str('-'.join(stime) + ' ' + data[i][BUS_API_TIME_STAMP].split(" ")[1])	
		    
		    stime =  dt.datetime.strptime(str_stime, "%Y-%m-%d %H:%M:%S")
		    delta = max_ts - stime
                    		    
                    if delta < dt.timedelta(0, 3600):
	                    
                        if flag_action:
                            query = query + ","

                        flag_action = True
 			


		    	query = query + " (%r,%r,%r,%f,%f,%r,%r)" % (str(data[i][BUS_API_BUSCODE]), str(data[i][BUS_API_BUSLINE]),'BUS',data[i][BUS_API_LATITUDE],data[i][BUS_API_LONGITUDE],str(hashlib.md5(str(data[i][BUS_API_TIME_STAMP])+str(data[i][BUS_API_BUSCODE])).hexdigest()),str_stime)

                    	#if not (i+1) == len(data):
                            #query = query + ","

		    		    
		if flag_action:
			insert_table(query)
		
		time.sleep(SLEEP_TIME)
		#"""
        #except SystemExit:
        #        break
        except Exception:
                error = "Error BUS API"
                print time.ctime()
                print error

 		time.sleep(SLEEP_TIME)
                
                #print str(sys.exc_info())
		if DEBUG:
                	out_log.write(error)
                	out_log.write(str(sys.exc_info()[0]))

                #continue
		pass

        try:

		query = "select table_name from information_schema.tables where table_schema = 'api' and table_name not in ('bus_rio','buscode')";
        	api_tables = execute_query_db(query)

                date_ref =  ''.join(str(max_ts)[0:10].split("-"))               
                hour_ref = str(max_ts).split(" ")[1].split(":")[0]              
		reference = int(date_ref) * 100 + int(hour_ref) - DELTA_HOUR
		print "Ref: " + str(reference)
				
		for i in range(len(api_tables)):
						
			table_time = ''.join(str(api_tables[i][0])[8:19].split("_")) 
			
			if reference > int(table_time):
				
				query = "drop table api." + api_tables[i][0]
				print query
				insert_table(query)

		time.sleep(SLEEP_TIME)
        except SystemExit:
                break
        except:
                error = "Error Dropping Out of date  Tables"
                print time.ctime()
                print error

                print str(sys.exc_info())
		if DEBUG:
                	out_log.write(error)
                	out_log.write(str(sys.exc_info()[0]))

                continue


        msg = "-------------------\nEnd PostgreSQL Func at %s\n-------------------" % time.ctime()

        print msg
	if DEBUG:
        	out_log.write(msg)

