#!/bin/bash
#This job performs spark submits via cron jobs.
#Spark history server helps get detailed information of jobs that have finished


if [ `hostname` == 'node0' ]
then

  set -x

  #Create spark events directory in dsefs
  dse hadoop fs -mkdir -p /spark/events/
  #dse hadoop fs -mkdir /spark
  #dse hadoop fs -mkdir /spark/events

  #Start Spark Job Server with custom config
  #sudo dse spark-history-server start --properties-file /tmp/datastax-sketch-examples/dse-sketching-demo/history-server-config/spark-defaults.conf
  
fi
