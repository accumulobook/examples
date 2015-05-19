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
package com.accumulobook.designs.lexicoder;

import com.google.common.net.InetAddresses;
import com.accumulobook.ExampleMiniCluster;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

public class CustomLexicoderExample {
  
  public static void main(String[] args) throws Exception {
    
    Connector conn = ExampleMiniCluster.getConnector();
    
    List<String> addrs = new ArrayList<>();
    
    addrs.add("192.168.1.1");
    addrs.add("192.168.1.2");
    addrs.add("192.168.11.1");
    addrs.add("192.168.11.11");
    addrs.add("192.168.11.100");
    addrs.add("192.168.11.101");
    addrs.add("192.168.100.1");
    addrs.add("192.168.100.2");
    addrs.add("192.168.100.12");
    
    conn.tableOperations().create("addresses");
    
    BatchWriter writer = conn.createBatchWriter("addresses", new BatchWriterConfig());
    
    // ingest using just address strings
    for(String addrString : addrs) {
      
      Mutation m = new Mutation(addrString);
      m.put("", "address string", addrString);
      
      writer.addMutation(m);
    }
    
    writer.flush();
    
    System.out.println("sort order using strings");
    Scanner scanner = conn.createScanner("addresses", Authorizations.EMPTY);
    for(Map.Entry<Key, Value> e : scanner) {
      System.out.println(e.getValue());
    }
    
    // delete rows
    conn.tableOperations().deleteRows("addresses", null, null);
    
    
    // ingest using lexicoder
    Inet4AddressLexicoder lexicoder = new Inet4AddressLexicoder();
    
    for(String addrString : addrs) {
      
      InetAddress addr = InetAddresses.forString(addrString);
      
      byte[] addrBytes = lexicoder.encode((Inet4Address)addr);
      
      Mutation m = new Mutation(addrBytes);
      m.put("", "address string", addrString);
      
      writer.addMutation(m);
    }
    
    writer.close();
    
    // scan again
    System.out.println("\nsort order using lexicoder");
    for(Map.Entry<Key, Value> e : scanner) {
      System.out.println(e.getValue());
    }
  }
}
