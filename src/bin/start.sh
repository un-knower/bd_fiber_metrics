#!/usr/bin/env bash

nohup sudo -u yarn
spark2-submit
    --class bd_fiber_metrics.App
    --name fiber --master yarn
    --conf spark.sql.shuffle.partitions=18
    --driver-class-path ../config/:../lib/mysql-connector-java-6.0.2.jar --jars ../lib/mysql-connector-java-6.0.2.jar,../lib/jedis-2.9.0.jar
    --conf "spark.executor.extraClassPath=/data/cloudera/parcels/CDH/lib/hive/lib/*"
    --num-executors 3
    --queue root.users.spark  ../apps/bd_fiber_metrics-1.0-SNAPSHOT.jar
    > ../apps/logs/log.log 2>&1 &
