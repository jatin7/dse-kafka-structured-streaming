---
title: Demo Application
menu:
  main:
      parent: Demo
      identifier: Demo Application
      weight: 101
---


Now that you have your Public IP address you can access a few different apps that you can leverage:

* Trending Now Dashboard - <http://publicIP:8081/>
* Configuration - <http://publicIP:8081/config/>
* Zeppelin Notebook - <http://publicIP::8080/>
* DataStax OpsCenter - <http://publicIP:8888/>
* Solr Admin UI - <http://publicIP:8983/solr/>

---

## Sketching Application

### Trending Now Dashboard
You must start the streaming job to start the data feed consumer for this demo. You can use the configuration screen or manually kick of the script. Config Screen is located at <http://publicIP:8081/config/>
![alt text](https://raw.githubusercontent.com/michaelraney/datastax-sketch-examples/master/docsrc/static/images/config-screen.png "Config Screen")

### Trending Now Dashboard
This is your screen to walk users through the visualization of the demo. It consist of three seperate windows.

#### External links
Links are some additional resources to code/documentation. The links in the menu from left to right. Link to spark worker hosted on node0. Link to Zeppelin notebooks(a notebook for executing dynamic queries and showing code behind the demo). Link to github containing code for demo and datasketching examples in DSE.

![alt text](https://raw.githubusercontent.com/michaelraney/datastax-sketch-examples/master/docsrc/static/images/demo-link-menu.png "External Links")

#### Perspective 10 seconds
This view is showing the raw processing of the twitter feed. Breaking down each 10 second streaming window into Hyperloglog and countminsketch data structures. This view does not have trending hashtags because its not as vaulable within 10 seconds.

![alt text](https://raw.githubusercontent.com/michaelraney/datastax-sketch-examples/master/docsrc/static/images/perspective-10second.png "Perspective A 10 seconds")


#### Perspective 10 minutes
This view is showing a 'roll-up' sketch of 10 second windows into 10 minute windows. Each point represents 10 minutes and since our sketching datastuctures has associative properties, this is not merely a simple count, but a representation of unique users over ten minutes. Additionally, we have a view of top hashtags. This uses countminsketch to create a frequency of hashtags seen on incomming tweets. You are able to click these and view them on twitter.

![alt text](https://raw.githubusercontent.com/michaelraney/datastax-sketch-examples/master/docsrc/static/images/perspective-10min.png "Perspective A 10 minutes")

#### Architecture Window
##### Logical Architecture
This diagram demonstates how the application works. It communicates the flow of data through the system and the creation of multiple perspectives.  
##### Physical Architecture
A future state architecute outlining the various components of the architecture.

![alt text](https://raw.githubusercontent.com/michaelraney/datastax-sketch-examples/master/docsrc/static/images/demo-architecture.png "Architecture view")

## Zeppelin Notebook
The Zeppelin notebook contains scala code samples to build query results. This offers the ability to build some dynamic queries for the demo. Contains samples for countmin sketch and hyperloglog

![alt text](https://raw.githubusercontent.com/michaelraney/datastax-sketch-examples/master/docsrc/static/images/zeppelin-notebook.png "Zeppelin")

https://raw.githubusercontent.com/michaelraney/datastax-sketch-examples/master/docsrc/static/images/zeppelin-notebook.png

## OpsCenter
If needed for your demo, you can access OpsCenter and give a tour of it at <http://publicIP:8888/>:
