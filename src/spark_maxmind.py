'''
Created on May 16, 2017

@author: sunoyon
'''

from pyspark import SparkFiles
from pyspark.shell import sqlContext, sc
from pyspark.sql.types import Row, StructType, StructField, StringType

from argparse import ArgumentParser


def spark_set_common_conf(sc):
    sc._jsc.hadoopConfiguration().set("parquet.enable.summary-metadata", "false")
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

def ip2gps_partition(ips, db_path):
    from geoip2 import database
    def ip2gps(ip):
        try:
            response = reader.city(ip['ip'])
            city = Row(ip=ip['ip'], latitude=response.location.latitude, longitude=response.location.longitude)
        except Exception:
            city = Row(ip="-1", latitude=-1.0, longitude=-1.0)
        return city
    reader = database.Reader(SparkFiles.get(db_path))
    return [ip2gps(ip) for ip in ips]

if __name__ == '__main__':

    arg_parser = ArgumentParser(    
            "Maxmind ip lookup using Spark."
        )
    arg_parser.add_argument('-src', '--src', dest='src',
                            default=None,
                            help='Source directory of ip addresses.')
    arg_parser.add_argument('-dest', '--dest', dest='dest',
                            default=None,
                            help='Destination directory for ip and coordinate')
    arg_parser.add_argument('-mmdb', '--mmdb', dest='mmdb',
                            default=None,
                            help='Path of GeoIP2-City.mmdb')
    options = arg_parser.parse_args()
    print 'Running with args# ' + str(options)
    
    if options.src == None:
        print "Source directory of ip addresses is missing."
        exit(1)
    if options.dest == None:
        print "Destination directory is missing."
        exit(1)
    if options.mmdb == None:
        print "GeoIP2-City.mmdb file is missing."
        exit(1)
        
    src_dir = options.src
    dest_dir = options.dest
    mmdb_path = options.mmdb
        
    spark_set_common_conf(sc)
    sc.addFile(mmdb_path)
    
    ip_schema = StructType([StructField("ip", StringType(), True)])
     
    ip = sqlContext.read.format('com.databricks.spark.csv').options(header='false').load(src_dir, schema=ip_schema)
    ip_gps = ip.rdd.mapPartitions(lambda p: ip2gps_partition(p, mmdb_path)).toDF().filter("ip <> '-1'")
    
    ip_gps.write.format("com.databricks.spark.csv").mode("overwrite").option("header", "false").option("delimiter", "\t").save(dest_dir) 
