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
package com.accumulobook.tableapi;

import com.accumulobook.ExampleMiniCluster;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Example of taking a snapshot of a table and restoring from it
 * to recover missing information.
 * 
 * 
 */
public class SnapshotRestoreExample  {
	
	public static void main(String[] args) throws Exception {
		
		Connector conn = ExampleMiniCluster.getConnector();
		TableOperations ops = conn.tableOperations();
		
		// create an example table
		System.out.println("Creating example table");
		ops.create("myTable");
		ops.setProperty("myTable", "table.file.replication", "1");
		
		BatchWriterConfig config = new BatchWriterConfig();
		config.setMaxLatency(10, TimeUnit.SECONDS);
		config.setMaxMemory(1000000L);
		config.setMaxWriteThreads(10);
		
		BatchWriter writer = conn.createBatchWriter("myTable", config);
		
		for(int i=0; i < 10; i++) {
			String iString = Integer.toString(i);
			Mutation m = new Mutation(iString);
			m.put("", "", iString);
			writer.addMutation(m);
		}
		
		writer.flush();
		
		Scanner scan = conn.createScanner("myTable", new Authorizations());
		for(Map.Entry<Key, Value> kv : scan) {
			System.out.println(
					kv.getKey().getRow() + "\t" + 
					new String(kv.getValue().get()));
		}
		
		// clone the table as a snapshot
		System.out.println("Creating snapshot");
		boolean flush = true;
		Map<String,String> propsToSet = new HashMap<>();
		Set<String> propsToExclude = new HashSet<>();

		String timestamp = Long.toString(System.currentTimeMillis());
		String snapshot = "myTable_" + timestamp;
		ops.clone("myTable", snapshot, flush, propsToSet, propsToExclude);
		
		for(String t : ops.list()) {
			System.out.println(t);
		}
		
		// make some mistaken deletes
		System.out.println("Deleting data from original");
		for(int i=0; i < 5; i++) {
			String iString = Integer.toString(i);
			Mutation m = new Mutation(iString);
			m.putDelete("", "");
			writer.addMutation(m);
		}
		
		writer.flush();
		
		// reusing our original scanner
		for(Map.Entry<Key, Value> kv : scan) {
			System.out.println(
					kv.getKey().getRow() + "\t" + 
					new String(kv.getValue().get()));
		}
		
		// restore from a snapshot
		System.out.println("Restoring from snapshot");
		ops.delete("myTable");
		ops.clone(snapshot, "myTable", flush, propsToSet, propsToExclude);
		
		
		// any existing scanners will no longer work
		// get a new one
		scan = conn.createScanner("myTable", new Authorizations());
		for(Map.Entry<Key, Value> kv : scan) {
			System.out.println(
					kv.getKey().getRow() + "\t" + 
					new String(kv.getValue().get()));
		}
		
		for(Map.Entry<String, String> p : ops.getProperties("myTable")) {
			if(p.getKey().equals("table.file.replication"))
				System.out.println(p.getValue());
		}
	}
}
