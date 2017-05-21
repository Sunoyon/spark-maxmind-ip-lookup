# Spark-Maxmind-Ip-Lookup
Clustered Maxmind Ip lookup system using Apache Spark.

## Version
Apache Spark 2+

## How to use

This repository can be tested using [Maxmind GeoLite2 City Free Downloadable Database](http://dev.maxmind.com/geoip/geoip2/geolite2/).

**Spark submit command**

    spark-submit \
    --packages com.databricks:spark-csv_2.10:1.5.0 \
    spark-maxmind-ip-lookup/src/spark_maxmind.py \
    --src spark-maxmind-ip-lookup/test/input \
    --dest spark-maxmind-ip-lookup/test/output \
    --mmdb spark-maxmind-ip-lookup/test/GeoLite2-City.mmdb
