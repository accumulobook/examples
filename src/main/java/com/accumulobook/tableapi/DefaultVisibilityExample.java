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
import java.util.Map;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;

public class DefaultVisibilityExample {

  public static void main(String[] args) throws Exception {
    
    // get a connector as the root user
    Connector conn = ExampleMiniCluster.getConnector();
    
    // create an example table
    String exampleTable = "example";
    conn.tableOperations().create(exampleTable);
    
    // write some data with col vis and others without
    BatchWriterConfig config = new BatchWriterConfig();
    BatchWriter writer = conn.createBatchWriter(exampleTable, config);
    Mutation m = new Mutation("one");
    
    m.put("", "col1",  "value in unlabeled entry");
    m.put("", "col2",  new ColumnVisibility("public"), "value in public entry");
    m.put("", "col3",  new ColumnVisibility("private"), "value in private entry");
    
    writer.addMutation(m);
    writer.close();
    
    // add auths to root account
    conn.securityOperations().changeUserAuthorizations("root", new Authorizations("public", "private"));
    // scan with no auths
    System.out.println("\nno auths:");
    Scanner scan = conn.createScanner(exampleTable, Authorizations.EMPTY);
    for(Map.Entry<Key, Value> e : scan) {
      System.out.println(e);
    }
    
    // scan with public auth
    System.out.println("\npublic auth:");
    scan = conn.createScanner(exampleTable, new Authorizations("public"));
    for(Map.Entry<Key, Value> e : scan) {
      System.out.println(e);
    }
    
    // scan with public and private auth
    System.out.println("\npublic and private auths:");
    scan = conn.createScanner(exampleTable, new Authorizations("public", "private"));
    for(Map.Entry<Key, Value> e : scan) {
      System.out.println(e);
    }
    
    // turn on default visibility
    System.out.println("\nturning on default visibility");
    conn.tableOperations().setProperty(exampleTable, "table.security.scan.visibility.default", "x");
    
    // scan with no auths
    System.out.println("\nno auths:");
    scan = conn.createScanner(exampleTable, Authorizations.EMPTY);
    for(Map.Entry<Key, Value> e : scan) {
      System.out.println(e);
    }
    
    // scan with public auth
    System.out.println("\npublic auth:");
    scan = conn.createScanner(exampleTable, new Authorizations("public"));
    for(Map.Entry<Key, Value> e : scan) {
      System.out.println(e);
    }
    
    // scan with public and private auth
    System.out.println("\npublic and private auths:");
    scan = conn.createScanner(exampleTable, new Authorizations("public", "private"));
    for(Map.Entry<Key, Value> e : scan) {
      System.out.println(e);
    }
  }
}
