---
date: 2017-09-07T22:35:05-04:00
title: Getting Started
menu:
  main:
      parent: Build and Run
      identifier: Getting Started
      weight: 101
---

Example for how to use this asset:

## Startup Script

This Asset leverages
[simple-startup](https://github.com/jshook/simple-startup). To start the entire
asset run `./startup all` for other options run `./startup`

## Manual Usage:

### Creating Tables
cqlsh HOST -f cql/setupTables.cql

## Building and Run Demos
### Streaming App
mvn package -f dse-sketching-demo/dse-sketching-streaming-job/pom.xml

#### Streaming Twitter Feed Sketch
dse spark-submit --class com.se.stream.TwitterStreamSketchDemo --total-executor-cores 2 --executor-memory 2g datastax-sketch-examples/dse-sketching-demo/dse-sketching-streaming-job/target/dse-sketching-streaming-job-jar-with-dependencies.jar consumerKey  consumerSecret  accessToken  accessTokenSecret 

#### Rollup Batch Job
dse spark-submit --class com.se.rollup.DataSketchingRollup --total-executor-cores 2 --executor-memory 2g datastax-sketch-examples/dse-sketching-demo/dse-sketching-streaming-job/target/dse-sketching-streaming-job-jar-with-dependencies.jar"

### Demo UI
#### Build WAR
mvn clean package -f dse-sketching-demo/dse-sketching-demo/pom.xml

#### Run Spring Boot Web Container 
java -Xmx2g -Xms2g -Dserver.port=8081 -jar dse-sketching-demo/dse-sketching-demo/target/dse-sketching-demo.war 


