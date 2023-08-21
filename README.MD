## software versions

kafka_2.11-2.3.0 (kafka version 2.3.0)  
pyspark 2.3.4 (spark version 2.3.4)  
spark-sql-kafka-0-10 (spark kafka driver version 0.10)  
org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 (marven dependency co-ordinates for above driver)

## python notebook

notebooks\Realtime Streaming V3.ipynb

This notebook can be run via Jupyter notebook

## python script

src\RealtimeStreamingV3.py

This script should be run via spark-submit as follows:

cd src
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 .\RealtimeStreamingV3.py