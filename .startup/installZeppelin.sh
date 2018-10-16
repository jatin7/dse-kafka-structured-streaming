#!/bin/bash

# Download Zeppelin from Google Drive
# Zeppelin distribution by doanduyhai http://www.doanduyhai.com/blog/?p=2325

# If we are on node0 install Zeppelin, copy Notebooks and start it up.
if [ `hostname` == 'node0' ]
then
  echo "Downloading Zeppelin..."
  curl -s -o zeppelin-0.7.1.tar.gz 'https://s3.amazonaws.com/dse-sketch-examples/zeppelin-0.7.1-dse-5.1.1.tar.gz' -L 2>&1 | tee zepplin-download.log

  echo "Untar Zeppelin..."
  tar -zxf zeppelin-0.7.1.tar.gz

  #Zeppelin Notebook API.
  zeppelin-0.7.1/bin/zeppelin-daemon.sh start

  #Although start is synchronous, I'll give it some time anyway
  sleep 5

  #XML Parser
  sudo apt-get install jq

  #Initialize Zeppelin.
  curl -o zepplinhome.out "http://localhost:8080/#/" -L -m 10
  curl -o zepplininterp.out "http://localhost:8080/#/interpreter" -L -m 10

  #Cassandra interpreter id
  CASSANDRA_INTERP_ID=$(curl localhost:8080/api/interpreter/setting | jq '.body|.[]|select(.name=="cassandra")|.id' -r)

  #Cassandra interpreter settings
  #Modify localhost
  curl localhost:8080/api/interpreter/setting | jq '.body|.[]|select(.name=="cassandra")|setpath(["properties","cassandra.cluster"]; "Cluster 1")|setpath(["properties","cassandra.hosts"]; "node0")|del(.id)' > cassandra-settings.json

  #Update Interpreter Settings
  curl -vX PUT "http://node0:8080/api/interpreter/setting/$CASSANDRA_INTERP_ID" -d @cassandra-settings.json \--header "Content-Type: application/json"

  #Spark interpreter id
  SPARK_INTERP_ID=$(curl localhost:8080/api/interpreter/setting | jq '.body|.[]|select(.name=="spark")|.id' -r)

  #Spark interpreter settings
  #Add cassandra host setting
  curl localhost:8080/api/interpreter/setting | jq '.body|.[]|select(.name=="spark")|setpath(["properties","spark.cassandra.connection.host"]; "node0")|del(.id)' > spark-settings.json

  #Update Interpreter Settings
  curl -vX PUT "http://node0:8080/api/interpreter/setting/$SPARK_INTERP_ID" -d @spark-settings.json \--header "Content-Type: application/json"

  sleep 5

  curl -vX POST http://localhost:8080/api/notebook/import -d @notebook/CountMinSketch/note.json \--header "Content-Type: application/json"
  curl -vX POST http://localhost:8080/api/notebook/import -d @notebook/HyperLogLog/note.json \--header "Content-Type: application/json"

  sleep 5

  zeppelin-0.7.1/bin/zeppelin-daemon.sh restart

  #Determine the localhost
  #MYIP=$(ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1'|grep -v "172.17.0.1")
  #node0
fi
