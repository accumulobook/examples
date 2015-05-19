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
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;

public class WholeRowIteratorExample {
  
  public static void main(String[] args) throws Exception {
    
    Connector conn = ExampleMiniCluster.getConnector();
    
    TableOperations ops = conn.tableOperations();
    ops.create("testTable");
    
    BatchWriter writer = conn.createBatchWriter("testTable", new BatchWriterConfig());
    
    for(int i=1; i < 100; i++) {
      Mutation m = new Mutation("row" + String.format("%02d", i));
      
      for(int j = 1; j < 100; j++) {
        // store the product of the row number and column number
        m.put("", "col" + j, Integer.toString(i * j));
      }
      
      writer.addMutation(m);
    }
    writer.flush();
    
    Scanner scanner = conn.createScanner("testTable", Authorizations.EMPTY);
    scanner.setRange(new Range("row50", "row60"));
    
    IteratorSetting setting = new IteratorSetting(30, "wri", WholeRowIterator.class);
    scanner.addScanIterator(setting);
    
    for(Map.Entry<Key, Value> e : scanner) {
      SortedMap<Key, Value> rowData = WholeRowIterator.decodeRow(e.getKey(), e.getValue());
      
      byte[] row = getRow(rowData);
      SortedMap<String, Value> columns = columnMap(rowData);
      
      System.out.println("\nrow\t" + new String(row));
      System.out.println(":col31\t" + columns.get(":col31"));
      System.out.println(":col15\t" + columns.get(":col15"));
    }
  }
  
  private static byte[] getRow(SortedMap<Key,Value> row) {
    return row.entrySet().iterator().next().getKey().getRow().getBytes();
  }
  
  private static SortedMap<String,Value> columnMap(SortedMap<Key,Value> row) {
    
    TreeMap<String,Value> colMap = new TreeMap<>();
    for(Map.Entry<Key, Value> e : row.entrySet()) {
      
      String cf = e.getKey().getColumnFamily().toString();
      String cq = e.getKey().getColumnQualifier().toString();
      
      colMap.put(cf + ":" + cq, e.getValue());
    }
    return colMap;
  }
}
