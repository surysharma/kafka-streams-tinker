# kafka Streams Sample application

This sample project explores various KStream topologies. We are using Scala KStreams DSL, more info about this can be found here https://kafka.apache.org/23/documentation/streams/developer-guide/dsl-api.html#scala-dsl

Before runnning the App, we need to create source and the sink topics.

$ ./kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic test_input_topic --partitions 3 --replication-factor 1
Created topic test_input_topic.

$ ./kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic test_output_topic --partitions 3 --replication-factor 1

$ ./kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic test_input_topic
>test


**What does this sample App do?**

This app mask the user credentials from an incoming transaction.



