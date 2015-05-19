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

public class CustomConstraintExample {

  public static void writeAndReportViolations(
          final BatchWriter writer,
          final Mutation m) {
    try {
      writer.addMutation(m);
      writer.flush();
      System.out.println("successfully written");
      
    } 
    catch (MutationsRejectedException ex) {
      
      List<ConstraintViolationSummary> violations = ex.getConstraintViolationSummaries();
      
      for(ConstraintViolationSummary v : violations) {
        System.out.println(v.getConstrainClass() +
        "\n" + v.getNumberOfViolatingMutations() +
        "\n" + v.getViolationDescription());
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    
    Connector conn = ExampleMiniCluster.getConnector();
    
    TableOperations ops = conn.tableOperations();
    ops.create("testTable"); 
    
    // add our custom constraint
    ops.addConstraint("testTable", ValidHeightWeightConstraint.class.getName());
    
    for(Map.Entry<String, Integer> c : ops.listConstraints("testTable").entrySet()) {
      System.out.println(c);
    }
    
    BatchWriter writer = conn.createBatchWriter("testTable", new BatchWriterConfig());
    
    // create an invalid mutation with only a height update
    Mutation m = new Mutation("person");
    m.put("", "height", "6.0");
    
    writeAndReportViolations(writer, m);
    
    // create a mutation with a valid height but 
    // an invalid weight value 
    m = new Mutation("person");
    m.put("", "height", "6.0");
    m.put("", "weight", "-200.0");
    
    // try to write
    writer = conn.createBatchWriter("testTable", new BatchWriterConfig());
    writeAndReportViolations(writer, m);
    
    // create a valid mutation this time
    m = new Mutation("person");
    m.put("", "height", "6.0");
    m.put("", "weight", "200.0");
    
    writer = conn.createBatchWriter("testTable", new BatchWriterConfig());
    writeAndReportViolations(writer, m);
    
    // write a mutation that has nothing to do with weight or height
    m = new Mutation("person");
    m.put("", "name", "Joe");

    writer = conn.createBatchWriter("testTable", new BatchWriterConfig());
    writeAndReportViolations(writer, m);
  }
}
