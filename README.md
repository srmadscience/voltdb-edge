<img title="Volt Active Data" alt="Volt Active Data Logo" src="http://52.210.27.140:8090/voltdb-awswrangler-servlet/VoltActiveData.png?repo=voltdb-edge">


# voltdb-edge - Edge computing command, control and communications

## Goal

This sandbox demonstrates Volt's capabilities as a command, control and communications ('C3') layer for IIoT and Edge processing. The IIot and Edge is leading to the creation of larger numbers of widely distributed devices, frequently peforming important functions but with patchy and limited connectivity. To make things more complicated not only do such networks have multiple kinds of devices with multiple communications protocols in use, but the devices themselves may not all be owned or under the control of one legal entity, so access and authentication also becomes an issue.

In this sandbox we pretend to have two kinds of devices, and two pretend power companies that take meter readings and perform other tasks. Power companies send messages to our system, and our job is to find the right meter, make sure they are allowed to access it and perform any needed format translations.

## Analysis

The key thing to understand is that this isn't a traditional database problem. A Database is where you store data when you aren't processing it. Our Smart Meter system doesn't store data for the sake of storing it - it stores it because the process of sending messages in an IIoT world is incredibly complex. I could draw a box on a whiteboard showing a device, and then an arrow into a cloud, and then another arrow to a power company. But that's about 1% of it. 

Let's look at a sample interaction.  Company 'A' wants to read meter 'B' in residence 'C'.:

### Downstream Message Flow


1. 'A' sends a message to us saying it wants to read the meter. The message is in a standard format.
2. The first thing we do is see if meter 'B' exists. It does, and lives on network segment 'S'. 
3. We then need to establish who has the right to read meter 'B'. It should be power company 'A', but it could also be that the owner has signed up to a price comparison web site that also has the right to read the meter.
4. Once we've established that Company 'A' has the right to read meter 'B' we need to figure out how to do that. Each device has its own rules for communication, and significant translation and formatting is needed. To make things more complicated meter 'B' was originally installed by power company 'Y', and nobody ib power company 'A' has  clue how to read it. Coming up with a viable message is our job.
5. Once we have the message, and it's ready to go, we then need to look at capacity constraints in the system. Because Company 'A' is very small we limit how many requests they can issue, in case they get hacked and someone decides to create a storm of requests. So maybe our request is sent ASAP, or maybe it goes into a queue. It could also go into a queue because the network segment 'S' (remember that?) is busy. One of the system's jobs is turning off meter's in burning buildings, so we can never let the network get saturated. We thus find ourselves doing  the kind of flow control and routing things that normally happen in the lower levels of the network stack.
6. The message travels through segment 'S' to the meter, that generates a response.

### Upstream message flow


1. The meter sends an encoded message with a reading to the system
2. The system has to figure out what kind of meter it is, what the request is, and translate it into a common format
3. It has to match the upstream response to a downstream request. Virtually all messages are reactive, so if they appear out of thin air something suspicious is happening.
4. We put the response into a queue of messages for power company 'A'.

### So what are the takeaways?

* This is a complex problem space
* There are multiple complicated steps for each business event. It's not an "Internet Plumbing" problem. 
* Implementation is non-trivial

### Why is it complicated?

#### Requirement for 'exactly once' messaging

Most messaging systems (e.g. Kafka) are "At least once" - if anything goes wrong it sends the message again. A minority (e.g. Akka) are 'at most once' - you'll never get a duplicate, but you may never get the message at all.... This problem needs 'exactly once' message semantics. 'Exactly once' is a transaction, so you need a platform that supports transactions.

#### Need for scale and performance

Using a separate client and data store creates significant performance and complexity problems, especially if you have a contractual obligation to be paranoid. To solve this using a traditional RDBMS or a document store would lead to thousands of lines of code that simply existed to make sure the data wasn't changing underneath you. What you actually need to do is to do all of your complex processing in the same place as your data in one go, You need Volt.

#### You need to understand the data

Streaming technologies such as Kafka abstract the data, which is a double edged sword.
Kafka became enormously successful by focusing on the 'plumbing' aspects of moving data, without ever understanding what the data itself was. This means that in applications where the content of the data is complicated you need to be able to parse, translate, reformat and summarize complex events at the speed they are being generated. You also need to scale without having to write additional code to do so. Volt can look like Kafka from the outside but solve these problems on the inside.

#### You need an approach that co-locates data and processing.

If you have a requirement for accuracy and precision you also have a  requirement for state management and transactions, just not an obvious one.  And if you need state management while doing non-trivial work you have a choice between a client/database architecture, which will lead to every single network message potentially turning into dozens of client/server interactions and many milliseconds per event, or you can do what Volt does, which is co-locate your decision making logic with your data and solve the problem in microseconds. Your incoming record is loaded into RAM in the same place all the other data needed to solve your problem is. You then run some Java to solve your business problem, which leads to an outbound record with the correct values.

###  What kinds of streaming problems is Volt useful for?

   * Aggregation, trivial and custom - Condensing many input records into fewer output records
   * Correlation - Joining 2 or more streams of input records that may not be easy to join
   * Duplicate Detection  - turning 'at least once' messaging into 'exactly once' messaging, when duplicate messages will cause problems. Duplicate detection is a much bigger problem than it looks, as you may need to keep data for a month or so.
   * Counting - Maintaining 100% accurate running totals. If you ask an RDBMS for a count you get the number when the query started running, which may be out of date. if you ask volt you get the actual number as of when the query finished executing,. 
   * Enrichment - translating, modifying or updating incoming records so the downstream records are immediately useful and don't need expensive downstream processing.

