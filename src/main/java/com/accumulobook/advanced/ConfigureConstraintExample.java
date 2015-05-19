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
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Mutation;

public class ConfigureConstraintExample {

  public static void main(String[] args) throws Exception {
    
    Connector conn = ExampleMiniCluster.getConnector();
    
    TableOperations ops = conn.tableOperations();
    ops.create("test");
    
    for(Map.Entry<String, Integer> c : ops.listConstraints("test").entrySet()) {
      System.out.println(c);
    } 
    
    // create a column qualifier that is 5MB in size
    
    StringBuilder sb = new StringBuilder();
    for(int i=0; i < 1024 * 1024; i++) {
      sb.append("LARGE");
    }
    
    String largeColQual = sb.toString();
    
    BatchWriter writer = conn.createBatchWriter("test", new BatchWriterConfig());
    Mutation m = new Mutation("test");
    
    m.put("", largeColQual, "");
    
    try {
      writer.addMutation(m);
      writer.flush();
      System.out.println("successfully written");
      
    } catch (MutationsRejectedException ex) {
      
      List<ConstraintViolationSummary> violations = ex.getConstraintViolationSummaries();
      
      for(ConstraintViolationSummary v : violations) {
        System.out.println(v.getConstrainClass() +
        "\n" + v.getNumberOfViolatingMutations() +
        "\n" + v.getViolationDescription());
      }
    }
    
    // remove constraint and try again
    ops.removeConstraint("test", 1);
    
    for(Map.Entry<String, Integer> c : ops.listConstraints("test").entrySet()) {
      System.out.println(c);
    } 
    
    writer = conn.createBatchWriter("test", new BatchWriterConfig());
    
    try {
      writer.addMutation(m);
      writer.flush();
      System.out.println("successfully written");
      
    } catch (MutationsRejectedException ex) {
      
      List<ConstraintViolationSummary> violations = ex.getConstraintViolationSummaries();
      
      for(ConstraintViolationSummary v : violations) {
        System.out.println(v.getConstrainClass() +
        "\n" + v.getNumberOfViolatingMutations() +
        "\n" + v.getViolationDescription());
      }
    }
  }
}
