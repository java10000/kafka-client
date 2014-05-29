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

public interface KafkaProperties
{
  final static String zkConnect = "172.16.1.5:4180,172.16.1.5:4181,172.16.1.5:4182";
  final static String brokerList = "172.16.1.5:9092,172.16.1.5:9093,172.16.1.5:9094";
  
//  final static String brokerList = "172.16.1.5:9092";
  
  final static  String groupId = "group0";
  final static String topic = "mykafka";
  final static String kafkaServerURL = "172.16.1.5";
  final static int kafkaServerPort = 9092;
  final static int kafkaProducerBufferSize = 64*1024;
  final static int connectionTimeOut = 100000;
  final static int reconnectInterval = 10000;
  final static String topic2 = "topic2";
  final static String topic3 = "topic3";
  final static String clientId = "SimpleConsumerDemoClient";
}
