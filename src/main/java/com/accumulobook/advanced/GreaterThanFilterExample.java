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
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Random;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;

public class GreaterThanFilterExample extends Filter {
  
  private static final String GREATER_THAN_CRITERION = "greaterThanOption";

  private long threshold = 0;
  
  @Override
  public boolean accept(Key k, Value v) {
    try {
      long num = Long.parseLong(new String(v.get()));
      return num > threshold;
    }
    catch(NumberFormatException ex) {
      // continue and return false
    }
    
    return false;
  }
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if (options.containsKey(GREATER_THAN_CRITERION)) {
      String gtString = options.get(GREATER_THAN_CRITERION);

      threshold = Long.parseLong(gtString);
    }
  }
  
  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions opts = super.describeOptions();
    opts.addNamedOption(GREATER_THAN_CRITERION, "Only return values greater than given numerical value");
    return opts;
  }
  
  @Override
  public boolean validateOptions(Map<String,String> options) {
    if(!super.validateOptions(options) || !options.containsKey(GREATER_THAN_CRITERION)) {
      return false;
    }
    
    String gtString = options.get(GREATER_THAN_CRITERION);
    try {
      Long.parseLong(gtString);
    }
    catch (NumberFormatException e) {
      return false;
    }
    
    return true;
  }
  
  public static void setThreshold(
          final IteratorSetting setting, 
          final int threshold) {
    
    setting.addOption(GREATER_THAN_CRITERION, Integer.toString(threshold));
  }
  
  public static void main(String[] args) throws Exception {
    
    Random random = new Random();
    
    Connector conn = ExampleMiniCluster.getConnector();
    TableOperations ops = conn.tableOperations();

    ops.create("testTable");
    
    // insert some data
    BatchWriter writer = conn.createBatchWriter("testTable", new BatchWriterConfig());
  
    for(int i=0; i < 30; i++) {
      
      int rowNum = random.nextInt(100);
      int colNum = random.nextInt(100);
      int value = random.nextInt(100);
      
      Mutation m = new Mutation("row" + rowNum);
    
      m.put("", "col" + colNum, Integer.toString(value));
      writer.addMutation(m);
    }
    
    writer.flush();
    
    
    IteratorSetting setting = new IteratorSetting(15, "gtf", GreaterThanFilterExample.class.getName());
    GreaterThanFilterExample.setThreshold(setting, 80);
    
    conn.tableOperations().attachIterator("testTable", setting);
    
    // we could, instead, set our iterator just for this scan
    //scanner.addScanIterator(setting);
    
    // check for the existence of our iterator
    for(Map.Entry<String, EnumSet<IteratorUtil.IteratorScope>> i : conn.tableOperations().listIterators("testTable").entrySet()) {
      System.out.println(i);
    }
    
    // scan whole table
    Scanner scanner = conn.createScanner("testTable", Authorizations.EMPTY);
    for(Map.Entry<Key, Value> e : scanner) {
      System.out.println(e);
    }
  }
}
