/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.oreilly.accumulo.integration;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;

public class KafkaIngestClient {
	private final ProblemMessageSaver saver;

	public interface ProblemMessageSaver {
		
		void save(final List<byte[]> messages);	
	}
	
	private final KafkaStream<byte[], byte[]> stream;
	private final BatchWriter batchWriter;
	private final int batchFlushSize;
	private final Function<byte[], Mutation> messageConverter;
	private final ConsumerConnector consumerConnector;
	private final ArrayList<byte[]> messageBuffer;

	public KafkaIngestClient(
			
			final String zookeeper,
			final String consumerGroup,
			final String topic,
			final String table,
			final BatchWriterConfig bwc,
			final Connector conn,
			final int batchFlushSize,
			final Function<byte[], Mutation> messageConverter,
			final ProblemMessageSaver saver) throws TableNotFoundException {
		
		// create kafka consumer
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("auto.offset.reset", "smallest");
		props.put("autocommit.enable", "false");
		props.put("group.id", consumerGroup);
		

		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		consumerConnector =
				kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
		
		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[],byte[]>>> consumerMap = 
				consumerConnector.createMessageStreams(topicCountMap);
		
		stream =  consumerMap.get(topic).get(0); 
		
		
		// create Accumulo batch writer
		batchWriter = conn.createBatchWriter(table, bwc);
		this.batchFlushSize = batchFlushSize;
		this.messageConverter = messageConverter;
		this.messageBuffer = new ArrayList<>();
		this.saver = saver;
	}
	
	public void run() {
		
		for(MessageAndMetadata<byte[], byte[]> mm : stream) {
			
			byte[] message = mm.message();
			messageBuffer.add(message);
			
			if(messageBuffer.size() >= batchFlushSize) {
				while (true) {
					try {
						batchWriter.addMutations(Iterables.transform(messageBuffer, messageConverter));
						batchWriter.flush();
						consumerConnector.commitOffsets();
						messageBuffer.clear();
						break;
					} catch (MutationsRejectedException ex) {

						if (ex.getConstraintViolationSummaries().size() > 0
								|| ex.getAuthorizationFailuresMap().size() > 0) {
							// retrying won't help
							saver.save(messageBuffer);
							messageBuffer.clear();
							break;
						}
						// else will retry until success
					}
				}
			}
		}
	}
}
