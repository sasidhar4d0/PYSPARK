__author__ = 'vn50fti'

import sys
import os
from src import *
from pyspark.sql.types import *
from pyspark.sql.functions import broadcast
from multiprocessing import Process, Pool
from multiprocessing.pool import ThreadPool
import logging

if os.path.exists('src.zip'):
    sys.path.insert(0,'src.zip')
else:
    sys.path.insert(0,'./src.zip')

try:
    from pyspark.sql import SparkSession
    from src.main.utility import generic_functions
    from src.main.procedures.sp_map import headroom_map
    from src.main.procedures.sp_coo import headroom_coo
    from src.main.procedures.sp_chart import headroom_chart
    from src.main.constants import constants
except ImportError as e:
     print ("Error importing Spark Modules", e)
     sys.exit(1)


# Initiate spark session variables
spark = SparkSession\
    .builder\
    .appName("User Defined Preferences Caching on PySpark")\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
    .config("spark.yarn.maxAppAttempts","1")\
    .config("yarn.resourcemanager.am.max-attempts",'1')\
    .config("spark.sql.shuffle.partitions",'400')\
    .config("spark.scheduler.mode","FAIR")\
    .enableHiveSupport()\
    .getOrCreate()

# Params Declaration

output_map1 = []
output_map2 = []
error_map = []
error_hive = []
keys = []
error_keys = []
numThreads = 10

# Schema for final tables

cSchema = StructType([StructField("ID", StringType()), StructField("RESULT_TXT", StringType())])
eSchema = StructType([StructField("ID",StringType())])

# MultiThreading

threading_cache = ThreadPool(numThreads)
threading_hive = ThreadPool(numThreads)

# Convert hive table into list[filter_hashcode_id, filter_condition_txt]

hive_params = generic_functions.getProcedureMappingTable(spark)

# Convert hive params to stored procedure params

sp_params = generic_functions.generateProcedureParams(hive_params)

# Broadcast Spark Tables

broadcast(spark.table("spine_poc.us_wm_tables_cvm_supp_diversity"))
broadcast(spark.table("spine_poc.us_wm_tables_vendor_cvm_supp"))
broadcast(spark.table("spine_poc.us_wm_skp_tables_skp_item_hierarchy"))
broadcast(spark.table("SPINE_POC.us_wc_skp_tables_skp_bu_dmn_item_wkly_summ"))
broadcast(spark.table("spine_poc.us_coc"))
broadcast(spark.table("spine_poc.ussams_coc"))

# Functions

def cacheHiveTable(table):
    spark.sql('CACHE TABLE SPINE_POC.' + table).repartition(50)

def executeHiveQuery(x):
    """ Function that take spark object and query as input and return the output"""
    query_output = spark.sql(x[1]).toJSON().collect()
    print([str(x[0]),'['+str(query_output)+']'.replace('"WALMART_SPINE_REDESIGN"','null').replace('-123454321.990','null').replace('-123454321.99','null').replace('-123454321','null'),str(x[2])])
    output_map2.append([str(x[0]),'['+str(query_output)+']'.replace('"WALMART_SPINE_REDESIGN"','null').replace('-123454321.990','null').replace('-123454321.99','null').replace('-123454321','null')])
    return [str(x[0]),'['+str(query_output)+']'.replace('"WALMART_SPINE_REDESIGN"','null').replace('-123454321.990','null').replace('-123454321.99','null').replace('-123454321','null'),str(x[2])]

def runMultiProcessing(threadParam,func,params):
    try:
        threadParam.map(func,params)
    except:
        print("Error during query execution")
        pass
    finally:
        threadParam.close()
        threadParam.join()

def checkKeysToRunAgain(actual,current):
    temp1 = []
    temp2 = []
    for i in current:
        temp1.append(i[0])
    for i in actual:
        if i[0] not in temp1:
            temp2.append(i)
    return temp2


def generateHiveQueries(sp_params,keys,error_map,error_keys):
    for i in sp_params:
        """# 0 - Key 1 - [15] 2  - SP"""
        if i[1][14] in ['US','USSAMS']:
            if 'MAP' in i[2] and len(i[1]) == 15:
                query = headroom_map(i[1]).frameQuery()
                #op = generic_functions.executeHiveQuery(spark,query)
                keys.append([str(i[0]),query,i[2]])
            elif 'CHART' in i[2] and len(i[1]) == 15:
                query = headroom_chart(i[1]).frameQuery()
                #op = generic_functions.executeHiveQuery(spark,query)
                keys.append([str(i[0]),query,i[2]])
            elif 'COO' in i[2] and len(i[1]) == 15:
                query = headroom_coo(i[1]).frameQuery()
                #op = generic_functions.executeHiveQuery(spark,query)
                keys.append([str(i[0]),query,i[2]])
            elif ('MAP' in i[2] or 'COO' in i[2] or 'CHART' in i[2]) and len(i[1]) != 15:
                error_map.append([str(i[0]),i[1],i[2]])
        else:
            error_keys.append(i)

# Caching tables in spark using multi-threading

spark.sql('cache table spine_poc.us_wm_skp_tables_skp_bu_dmn_item_wkly_summ').repartition(200)

try:
    threading_cache.map(cacheHiveTable,constants.hive_table_cache)
except:
    print("Error while spark caching")
finally:
    threading_cache.close()
    threading_cache.join()

# Generating hive queries and adding to a list

generateHiveQueries(sp_params,keys,error_map,error_keys)

print('Length of keys is {}'.format(len(keys)))

print('Printing keys ')

print(keys)

print("ID's that has issues with market Key")

print(error_keys)

print('Length of params <>15 is {}'.format(len(error_map)))

print("printing records having params <> 15")

print(error_map)

print('MultiProcessing Started')

try:
    threading_hive.map(executeHiveQuery,keys)
except:
    print("Error during query execution")
    pass
finally:
    threading_hive.close()
    threading_hive.join()
    print('Length of output map {}'.format(len(output_map2)))

counter = [len(keys)]

for i in range(10):
    print('Calculating failed queries attempt {}'.format(i))
    print('Length of keys {}'.format(len(keys)))
    print('Length of output_map2 {}'.format(len(output_map2)))
    failed_queries = checkKeysToRunAgain(keys,output_map2)
    print('Length of failed queries {}'.format(len(failed_queries)))
    counter.append(len(failed_queries))
    print('Printing counter {}'.format(counter))
    thread_hive=ThreadPool(numThreads)
    if counter[i]  > counter[i+1]:
        print('Running Multi Processing attempt {}'.format(i))
        try:
            thread_hive.map(executeHiveQuery,failed_queries)
        except:
            print("Error during query execution attempt {}".format(i))
            pass
        finally:
            thread_hive.close()
            thread_hive.join()
    else:
        print('Printing failed queries')
        print(failed_queries)
        break

try:
    spark.createDataFrame(spark.sparkContext.parallelize(output_map2),cSchema).write.mode("overwrite").saveAsTable("spine_poc.poc100_success_map2")
except:
    print("Error during success_map final table creation")

try:
    spark.createDataFrame(spark.sparkContext.parallelize(error_map),eSchema).write.mode("overwrite").saveAsTable("spine_poc.poc100_error_map")
except:
    print("Error during error_map final table creation")
    print("Printing output map 2 - Hive query output")
    print(output_map2)

spark.stop()
