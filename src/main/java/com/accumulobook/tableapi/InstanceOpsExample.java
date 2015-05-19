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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Example of looking at instance operations while there is 
 * table reading and writing activity.
 * 
 */
public class InstanceOpsExample {

  public static class DummyActivityThread implements Runnable {
    private final Connector conn;
    private final int i;
    private final Random random;

    public DummyActivityThread(final Connector conn, final int i) {
      this.conn = conn;
      this.i = i;
      random = new Random();
    }

    @Override
    public void run() {

      try {
        conn.tableOperations().create("table" + i);
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxLatency(1, TimeUnit.SECONDS);
        config.setMaxMemory(1000000L);
        config.setMaxWriteThreads(1);

        for (int j = 0; j < 5; j++) {
          try {
            Thread.sleep((long) (random.nextDouble() * 5000));
          } catch (InterruptedException ex) {
            Logger.getLogger(InstanceOpsExample.class.getName()).log(Level.SEVERE, null, ex);
          }

          BatchWriter writer = conn.createBatchWriter("table" + i, config);
          for (int k = 0; k < 100000; k++) {
            Mutation m = new Mutation(Integer.toString(k));
            m.put("", "dummy", "info");
            writer.addMutation(m);
          }
          //System.out.println("writing data to table " + i);
          writer.close();

          conn.tableOperations().compact("table" + i, null, null, true, false);

          try {
            Thread.sleep((long) (random.nextDouble() * 5000));
          } catch (InterruptedException ex) {
            Logger.getLogger(InstanceOpsExample.class.getName()).log(Level.SEVERE, null, ex);
          }

          Scanner scanner = conn.createScanner("table" + i, Authorizations.EMPTY);
          Key x;
          //System.out.println("scanning table " + i);
          for (Map.Entry<Key, Value> e : scanner) {
            x = e.getKey();
          }
        }

      } 
      catch (AccumuloException | AccumuloSecurityException | TableExistsException | TableNotFoundException ex) {
      }
    }
  }
  
  public static void main(String[] args)  throws Exception {
    final Connector conn = ExampleMiniCluster.getConnector();
    
    InstanceOperations instOps = conn.instanceOperations();
    
    System.out.println("Starting dummy activity and waiting 30 seconds ...");
    for(int i=0; i < 20; i++) {
      new Thread(new DummyActivityThread(conn, i)).start();
    }
    
    Thread.sleep(30000);
    
    System.out.println("Printing report of tablet server activity ...");
    
    List<String> tabletServers = instOps.getTabletServers();
    for(String tserver : tabletServers) {
      
      System.out.println("==== " + tserver + " ====");
      
      List<ActiveCompaction> compactions = instOps.getActiveCompactions(tserver);
      for(ActiveCompaction c : compactions) {
        System.out.println(
                "Active Compaction\n\n" 
                + "age:\t" + c.getAge() + "\n"
                + "entries read:\t" + c.getEntriesRead() + "\n"
                + "entries written:\t" + c.getEntriesWritten() + "\n"
                + "extent:\t" + c.getExtent() + "\n"
                + "input files:\t" + c.getInputFiles() + "\n"
                + "iterators:\t" + c.getIterators() + "\n"
                + "locality group:\t" + c.getLocalityGroup() + "\n"
                + "output file:\t" + c.getOutputFile() + "\n"
                + "reason:\t" + c.getReason() + "\n"
                + "table:\t" + c.getTable() + "\n"
                + "type:\t" + c.getType() + "\n\n");
      }
      
      List<ActiveScan> scans = instOps.getActiveScans(tserver);
      for(ActiveScan s : scans) {
        System.out.println(
                "Active Scan\n\n"
                + "age:\t" + s.getAge() + "\n"
                + "auths:\t" + s.getAuthorizations() + "\n"
                + "client:\t" + s.getClient() + "\n"
                + "columns:\t" + s.getColumns() + "\n"
                + "extent:\t" + s.getExtent() + "\n"
                + "idle:\t" + s.getIdleTime() + "\n"
                + "last contact:\t" + s.getLastContactTime() + "\n"
                + "scan id:\t" + s.getScanid() + "\n"
                + "server side iterator list:\t" + s.getSsiList() + "\n"
                + "server side iterator options:\t" + s.getSsio() + "\n"
                + "state:\t" + s.getState() + "\n"
                + "table:\t" + s.getTable() + "\n"
                + "type:\t" + s.getType() + "\n"
                + "user:\t" + s.getUser() + "\n\n");
      }
      
      
      try {
        instOps.ping(tserver);
        System.out.println("reachable");
      } catch (AccumuloException ex) {
        System.out.println("unreachable");
      }
      
      System.out.println();
    }
    
    System.out.println("Printing out site configuration ...");
    Map<String, String> siteConfig = instOps.getSiteConfiguration();
    for(Map.Entry<String, String> setting : siteConfig.entrySet()) {
      System.out.println(setting.getKey() + "\t" + setting.getValue());
    }
    
    System.out.println("Printing out system configuration ...");
    Map<String, String> sysConfig = instOps.getSystemConfiguration();
    for(Map.Entry<String, String> setting : sysConfig.entrySet()) {
      System.out.println(setting.getKey() + "\t" + setting.getValue());
    }
    
    System.out.println("Waiting for dummy threads to complete ...");
  }
}
