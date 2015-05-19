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
package com.accumulobook.advanced;

import com.accumulobook.ExampleMiniCluster;
import java.util.Map;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

public class LowLevelIteratorExample {

  public static void main(String[] args) throws Exception {
    
    Connector conn = ExampleMiniCluster.getConnector();
    
    TableOperations ops = conn.tableOperations();
    ops.create("testTable");
    
    BatchWriter writer = conn.createBatchWriter("testTable", new BatchWriterConfig());
    
    for(int i=0; i < 100; i++) {
      Mutation m = new Mutation("row" + String.format("%02d", i));
      
      for(int j = 0; j < 100; j++) {
        m.put("", String.format("col%02d", j), i + " " + j);
      }
      
      writer.addMutation(m);
    }
    writer.flush();
    
    Scanner scanner = conn.createScanner("testTable", Authorizations.EMPTY);
    
    // count items returned
    int returned = 0;
    for(Map.Entry<Key, Value> e : scanner) 
      returned++;
    
    System.out.println("items returned: " + returned);
    
    IteratorSetting setting = new IteratorSetting(30, "fci", FirstColumnIterator.class);
    scanner.addScanIterator(setting);
    
    returned = 0;
    for(Map.Entry<Key, Value> e : scanner) {
      System.out.println(e);
      returned++;
    }
    
    System.out.println("items returned: " + returned);
  }
}
