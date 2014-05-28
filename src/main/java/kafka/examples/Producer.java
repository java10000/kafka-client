/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;


import java.util.Properties;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Producer extends Thread
{
  private final kafka.javaapi.producer.Producer<Integer, String> producer;
  private final String topic;
  private final Properties props = new Properties();

  public Producer(String topic)
  { 
    // Defines where the Producer can find a one or more Brokers to determine the Leader for each topic. 
    // This does not need to be the full set of Brokers in your cluster but should include at least two 
    // in case the first Broker is not available. No need to worry about figuring out which Broker is the 
    // leader for the topic (and partition), the Producer knows how to connect to the Broker and ask for 
    // the meta data then connect to the correct Broker. Here, we are listing the three ports we created
    // that are listening as kafka brokers in a pseudo-clustered manner.
    props.put("metadata.broker.list", KafkaProperties.brokerList);
    
    // Defines what Serializer to use when preparing the message for transmission to the Broker. In our 
    // example we use a simple String encoder provided as part of Kafka. Note that the encoder must accept 
    // the same type as defined in the KeyedMessage object in the next step. 
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    
    // Tells Kafka that you want your Producer to require an acknowledgment from the Broker that the message 
    // was received. Without this setting the Producer will 'fire and forget' possibly leading to data loss.
    props.put("request.required.acks", "1");
    
    // Use random partitioner. Don't need the key type. Just set it to Integer.
    // The message is of type String.
    producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
    
    this.topic = topic;
    
  }
  
  public void run() {
    int messageNo = 1;
    while(true)
    {
      String messageStr = new String("Message_" + messageNo);
      producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
      messageNo++;
    }
  }

}
