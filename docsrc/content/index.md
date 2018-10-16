---
title: "What's Trending: DataStax Sketching Examples"
type: index
weight: 0
---
# What's Trending: DataStax Sketching Examples
This is a guide for how to use the What's Trending Demo for the DataStax Enterprise (DSE) platform. Data Sketching is key concept in a stream data processing architecture. In this demo we combine the power of spark streaming with the strengths of DSE multi-model data store to deliver a powerful real-time architecture for delivering actionable insights on trending data.

![alt text](
https://github.com/michaelraney/datastax-sketch-examples/raw/master/docsrc/static/images/both-perspectives.png "DemoScreen")


#### This Demo Requires a Twitter Developer App Key (Free)
To run this example, you will need to signup for the Twitter Developer API and use the appropriate credentials.  To retrieve the API key, API secret, Access token, and Access token secret, follow the steps below:

* Go to https://apps.twitter.com/ and log in with your twitter credentials.
* Click "Create New App"
* Fill out the form, agree to the terms, and click "Create your Twitter application"
* In the next page, click on "API keys" tab, and copy your "API key" and "API secret".
* Scroll down and click "Create my access token", and copy your "Access token" and "Access token secret".
* Not working? For more detailed instructions: http://docs.inboundnow.com/guide/create-twitter-application/
* Finally, when the demo starts go to seednode:8081/config to enter in your credentials

#### Recommended instances 
* m3.xlarge, m3.2xlarge


## Motivation
The motivation for this demo is to show how DataStax Enterprise can be used to deliver real-time insights on a live data feed to promote engagement with end users through transparency of user interactions. 

## What is included?
This field asset includes

* Analytics and Analytics Solo
* Spark Streaming
* Data Sketching Libraries
* Timeseries
* Zeppelin Notebook

## Business Take Aways
The power behind a 'what's trending' architecture is the value in that transparency promotes engagement. Users are more likely to click recommendations when they understand how the recommendation was created. Additionally, when users understand how recommendations are created they are more likely to participate in those interactions, such as likes and reviews, that contribute to the recommendation system. 

Businesses today want to broaden the experience of its end users. Often users are focused on their inner circles and by exposing trending content you bring those users out of their inner circles and can help draw attention to different content. 
DataStax provides both the compute layer to process streaming data and also the multi model store to persist the insights collected and deliver them in real-time with 24/7 business continuity.  

## Technical Take Always
This demo demonstrates how architects can take advantage of stream processing tradeoffs in DataStax Enterprise. By Sacrificing a 2% margin of error in accuracy you can gain linear performance on processing massing amounts of data in real-time. Many use cases such as trend analysis do not require 100% accuracy, but the ability to understand the nature, frequency and trends in data.

## Probabilistic data structures in space time accuracy tradeoffs. 
 In systems architecture, we have several design tradeoffs to efficiently guide our technology choices for enterprise solutions.  When reviewing these types of tradeoffs, it's important to note that tradeoffs are not absolutes, but guidelines to what will be simple and what will be complex. Probabilistic data structures trade accuracy with a margin of error while making use of a finite space in a short amount of time. This is the proven solution for determining trends and frequency distribution at scale. 

![alt text](https://raw.githubusercontent.com/michaelraney/datastax-sketch-examples/master/images/space-time-accuracy-tradeoff.png "Data Processing Tradeoffs")

### Pick two and let's go
Imagine you have exponentially growing data feeds consisting of multiple terabytes of detailed user interactions coming in to your system everyday. Since the business requires the technology to provide business action on insights for data in-flight, would you accept a 2% margin of error for predictable, scalable, and efficient performance? Most often the answer is Yes! 

### Sliding Window
When performing approximations on streaming data, you are performing an approximation on a data stream that is not finite, meaning there is no distinguishable end or beginning. We must ask ourselves, how will this data be accessed?  Looking at some real world situations, we want to compare user interactions and volumes with external events.  Examples could be measuring the number of unique website visitors after running a promotional advertisement during the World Series, or giving existing users visibility into what products are trending during the holiday season.

### Associative Properties of Probabilistic Data Structures
The probabilistic data structures are associative in nature. Associative property states that you can add or multiply regardless of how numbers are grouped. For example, if we store the approximation calculations in a time-series of 3 second intervals, using the associative properties of the data structure, we can later group the results to expand the calculations to larger time spans of 3 minutes, 3 hours, 3 months, 3 years, etc. 

![alt text](
https://raw.githubusercontent.com/michaelraney/datastax-sketch-examples/master/images/Approximations-timeseries.png "Timeseries")

## Sketch Examples
### HyperLogLog
  Hyperloglog addresses the problem in determining cardinality on large datasets. This is useful for efficiently summarizing streams of data such as number of unique visitors of a website or counting ad impressions.

### CountMinSketch
  Helpful in finding the frequency of items in a stream of data. In the following example we will take a look at estimating the frequency of the top 10 active Twitter users in a streaming feed of tweets.
  
## Technology Stack
* DataStax Enterprise with Spark Enabled
* Sketch Library - A library which provides abstractions for abstract algebra in the Scala programming language. In this example we will use Algebird, but from a Datastax perspective we are not limited to a specific sketching library. 

![alt text](
https://raw.githubusercontent.com/michaelraney/datastax-sketch-examples/master/images/Architecture.png "Architecture")


## Learn More
* Zeppelin and Datastax http://www.doanduyhai.com/blog/?p=2325 (Configuring Vanilla Zeppelin)
* Algebird https://github.com/twitter/algebird
* CountMinSketch Paper https://arxiv.org/pdf/1502.04885v1.pdf
* Cassandra TimeSeries Modeling https://academy.datastax.com/resources/getting-started-time-series-data-modeling


