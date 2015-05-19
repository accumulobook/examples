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
import com.accumulobook.advanced.RunningAverageCombiner.LongDoublePairEncoder;
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
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;



public class CustomCombinerExample {

  public static void main(String[] args) throws Exception {
    
    Connector conn = ExampleMiniCluster.getConnector();
    
    TableOperations ops = conn.tableOperations();
    ops.create("testTable");
    
    // remove versioning iterator
    ops.removeIterator("testTable", "vers", EnumSet.allOf(IteratorScope.class));
    
    // configure our iterator
    IteratorSetting setting = new IteratorSetting(10, "rac", RunningAverageCombiner.class);
    RunningAverageCombiner.setCombineAllColumns(setting, true);
    RunningAverageCombiner.setLossyness(setting, false);
    
    // attach to table for all scopes
    ops.attachIterator("testTable", setting);
    
    BatchWriter writer = conn.createBatchWriter("testTable", new BatchWriterConfig());
    
    // begin writing numbers to our table
    Random random = new Random();
    
    for(int i = 0; i < 5; i++) {
      Mutation m = new Mutation("heights");
      m.put("", "average", "1:" + (random.nextDouble() * 2 + 4.5));
      writer.addMutation(m);
    }
    writer.flush();
    
    LongDoublePairEncoder enc = new LongDoublePairEncoder();
    
    Scanner scanner = conn.createScanner("testTable", Authorizations.EMPTY);
    for(Map.Entry<Key, Value> e : scanner) {
      Pair<Long,Double> pair = enc.decode(e.getValue().get());
      
      double average = pair.getSecond() / pair.getFirst();
      System.out.println(average);
    }
    
    for(int i = 0; i < 100; i++) {
      Mutation m = new Mutation("heights");
      m.put("", "average", "1:" + (random.nextDouble() * 2 + 4.5));
      writer.addMutation(m);
    }
    writer.flush();
    
    for(Map.Entry<Key, Value> e : scanner) {
      Pair<Long,Double> pair = enc.decode(e.getValue().get());
      
      double average = pair.getSecond() / pair.getFirst();
      System.out.println(average);
    }
  }	
}
