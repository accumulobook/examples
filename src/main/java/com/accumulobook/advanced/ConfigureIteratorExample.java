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
import java.util.EnumSet;
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
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;

public class ConfigureIteratorExample {

  public static void main(String[] args) throws Exception {
    
    Connector conn = ExampleMiniCluster.getConnector();
    TableOperations ops = conn.tableOperations();

    ops.create("testTable");
    
    // insert some data
    BatchWriter writer = conn.createBatchWriter("testTable", new BatchWriterConfig());
    Mutation m = new Mutation("row");
    m.put("", "col", "1");
    writer.addMutation(m);
    
    m = new Mutation("row");
    m.put("", "col", "2");
    writer.addMutation(m);
    
    m = new Mutation("row");
    m.put("", "col", "3");
    writer.addMutation(m);
    writer.flush();
    
    
    // look at the key-value pair we inserted
    System.out.println("\nview with versioning iterator on");
    Scanner scanner = conn.createScanner("testTable", Authorizations.EMPTY);
    for(Map.Entry<Key, Value> e : scanner) {
      System.out.println(e.getKey() + ":\t" + e.getValue());
    }
    
    // list all iterators setup
    System.out.println("\niterators setup");
    Map<String, EnumSet<IteratorUtil.IteratorScope>> iters = ops.listIterators("testTable");
    for(Map.Entry<String, EnumSet<IteratorUtil.IteratorScope>> iter : iters.entrySet()) {
      System.out.println(iter.getKey() + ":\t" + iter.getValue());
    }
    
    // look at the settings for the versioning iterator
    IteratorSetting setting = ops.getIteratorSetting("testTable", "vers", IteratorScope.scan);
    
    System.out.println("\niterator options");
    for(Map.Entry<String, String> opt : setting.getOptions().entrySet()) {
      System.out.println(opt.getKey() + ":\t" + opt.getValue());
    }
    
    // disable the versioning iterator for all scopes
    ops.removeIterator("testTable", "vers", EnumSet.allOf(IteratorScope.class));
    
    // look at our table again
    System.out.println("\nview with versioning iterator off");
    for(Map.Entry<Key, Value> e : scanner) {
      System.out.println(e.getKey() + ":\t" + e.getValue());
    }
    
    // enable the SummingCombiner iterator on our table
    IteratorSetting scSetting = new IteratorSetting(15, "sum", SummingCombiner.class);
    
    // apply combiner to all columns
    SummingCombiner.setCombineAllColumns(scSetting, true);
    
    // expect string representations of numbers
    SummingCombiner.setEncodingType(scSetting, SummingCombiner.Type.STRING);
    
    ops.checkIteratorConflicts("testTable", scSetting, EnumSet.of(IteratorScope.scan));
    
    // attach the iterator
    ops.attachIterator("testTable", scSetting, EnumSet.of(IteratorScope.scan));
    
    // look at our table now
    System.out.println("\nview with summing combiner iterator on");
    for(Map.Entry<Key, Value> e : scanner) {
      System.out.println(e.getKey() + ":\t" + e.getValue());
    }
  }
}
