#!/bin/bash

#Create Tables and Solr Schema.
#

if [ `hostname` == 'node0' ]
then

  #Race Condition DSE has not started
  echo "Has DSE Started?"
  if ! nc -z node0 9042; then

    counter=0
    iterations=36
    sleepInterval=5

    echo "Waiting for DSE to start..."
    while  ! nc -z node0 9042; do

       echo "Iteration $counter :Sleep $sleepInterval seconds..."
       sleep $sleepInterval

       counter=$((counter+1))

       if [[ $counter -gt $iterations ]]; then
         echo "DSE not started exit..."
         break
       fi
    done
  fi

   cqlsh node0 -f cql/setupTables.cql

   #race condition
   sleep 10s
   #run repar for tables that changed replication factor
   #modified dsefs keyspace in cql startup script
   nodetool repair -pr

   sleep 10s
fi
