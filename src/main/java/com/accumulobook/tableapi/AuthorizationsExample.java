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
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.TablePermission;

public class AuthorizationsExample {
  
  public static void main(String[] args) throws Exception {
    
    // get a connector as the root user
    Connector adminConn = ExampleMiniCluster.getConnector();
    
    // get a security operations object as the root user
    SecurityOperations secOps = adminConn.securityOperations();
    
    // admin creates a new table and writes some data protected with Column Visibilies
    System.out.println("\n--- creating table ---");
    String safeTable = "safeTable";
    adminConn.tableOperations().create(safeTable);
    
    // admin writes initial data
    System.out.println("\n--- writing initial data ---");
    BatchWriterConfig config = new BatchWriterConfig();
    BatchWriter writer = adminConn.createBatchWriter(safeTable, config);
    Mutation m = new Mutation("safe001");
    
    // write information about this particular safe
    m.put("info", "safeName", new ColumnVisibility("public"), "Super Safe Number 17");
    m.put("info", "safeLocation", new ColumnVisibility("bankEmployee"), "3rd floor of bank 2");
    m.put("info", "safeOuterDoorCombo", new ColumnVisibility("bankEmployee&safeWorker"), "123-456-789");
    
    // store some information about bank owned contents stored in the safe
    m.put("contents", "box001", new ColumnVisibility("bankEmployee"), "bank charter");
    
    writer.addMutation(m);
    writer.close();
    
    // admin creates a new customer user
    String customer = "customer003";
    PasswordToken customerToken = new PasswordToken("customerPassword");
    secOps.createLocalUser(customer, customerToken);
    
    // set authorizations for user and grant permission to read and write to the safe table
    Authorizations customerAuths = new Authorizations("public", "customer003");
    secOps.changeUserAuthorizations(customer, customerAuths);
    secOps.grantTablePermission(customer, safeTable, TablePermission.READ);
   
    
    // get a connector as our customer user
    Connector customerConn = ExampleMiniCluster.getInstance().getConnector(customer, customerToken);
    
    // user attempts to get a scanner with authorizations not associated with the user
    System.out.println("\n--- customer scanning table for bank employee privileged information ---");
    Scanner scanner;
    try {
      scanner = customerConn.createScanner(safeTable, new Authorizations("public", "bankEmployee"));
      
      for(Map.Entry<Key, Value> e : scanner) {
        System.out.println(e);
      }
    }
    catch (Exception ex) {
      System.out.println("problem scanning table: " + ex.getMessage());
    }
    
    // user reads data with authorizations associated with the user
    System.out.println("\n--- customer scanning table for allowed information ---");
    scanner = customerConn.createScanner(safeTable, customerAuths);
    for(Map.Entry<Key, Value> e : scanner) {
      System.out.println(e);
    }
    
    
    // admin grants write permission to user
    secOps.grantTablePermission(customer, safeTable, TablePermission.WRITE);
    
    // user writes information only she can see to the table
    // describing the contents of a rented safety deposit box
    System.out.println("\n--- customer writing own information ---");
    BatchWriter userWriter = customerConn.createBatchWriter(safeTable, config);
    Mutation userM = new Mutation("safe001");
    userM.put("contents", "box004", new ColumnVisibility("customer003"), "jewelry, extra cash");
    userWriter.addMutation(userM);
    userWriter.flush();
    
    // scan to see the bank info and our own info
    System.out.println("\n--- customer scanning table for allowed information ---");
    scanner = customerConn.createScanner(safeTable, customerAuths);
    for(Map.Entry<Key, Value> e : scanner) {
      System.out.println(e);
    }
    
    // admin creates a new bank employee user
    String bankEmployee = "bankEmployee005";
    PasswordToken bankEmployeeToken = new PasswordToken("bankEmployeePassword");
    secOps.createLocalUser(bankEmployee, bankEmployeeToken);
    
    // admin sets authorizations for bank employee
    // and grants read permission for the table
    Authorizations bankEmployeeAuths = new Authorizations("bankEmployee", "public");
    secOps.changeUserAuthorizations(bankEmployee, bankEmployeeAuths);
    secOps.grantTablePermission(bankEmployee, safeTable, TablePermission.READ);
    
    // connect as bank employee
    Connector bankConn = ExampleMiniCluster.getInstance().getConnector(bankEmployee, bankEmployeeToken);
    
    // attempt to scan customer information
    System.out.println("\n--- bank employee scanning table for customer information ---");
    Scanner bankScanner;
    try {
      bankScanner = bankConn.createScanner(safeTable, new Authorizations("customer003"));
      
      for(Map.Entry<Key, Value> e : bankScanner) {
        System.out.println(e);
      }
    }
    catch (Exception ex) {
      System.out.println("problem scanning table: " + ex.getMessage());
    }

    // bank employee scans all information they are allowed to see
    System.out.println("\n--- bank employee scanning table for allowed information ---");
    bankScanner = bankConn.createScanner(safeTable, bankEmployeeAuths);
    
    for(Map.Entry<Key, Value> e : bankScanner) {
      System.out.println(e);
    }
    
    // bank employee scans using a subset of authorizations
    // to check which information is viewable to the public
    System.out.println("\n--- bank employee scanning table for only public information ---");
    bankScanner = bankConn.createScanner(safeTable, new Authorizations("public"));
    
    for(Map.Entry<Key, Value> e : bankScanner) {
      System.out.println(e);
    }
    
    // admin protects table against users writing new data they cannot read
    adminConn.tableOperations().addConstraint(safeTable, "org.apache.accumulo.core.security.VisibilityConstraint");
    
    // customer attempts to write information protected with a bank authorization
    // erasing the combination for the outer door of the safe
    System.out.println("\n--- customer attempting to overwrite bank information ---");
    try {
      userM = new Mutation("safe001");
      userM.put("info", "safeOuterDoorCombo", new ColumnVisibility("bankEmployee&safeWorker"), "------");
      userWriter.addMutation(userM);
      userWriter.flush();
    }
    catch(Exception e) {
      System.out.println("problem attempting to write data: " + e.getMessage());
    }
    
    
  }
}
