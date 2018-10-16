#!/bin/bash


if [ `hostname` == 'node0' ]
then

  echo "Building Steaming App... output to trending-app-build.log"

  #Build Twitter Streaming Project
  mvn clean package -f dse-sketching-demo/dse-sketching-demo/pom.xml > trending-app-build.log

  nohup java -Xmx1g -Xms1g -Dserver.port=8081 -jar dse-sketching-demo/dse-sketching-demo/target/dse-sketching-demo.war >> trending-app.out &

fi
