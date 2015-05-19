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
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;

public class PermissionsExample {

  public static void main(String[] args) throws Exception {
    
    // get a connector as the root user
    Connector adminConn = ExampleMiniCluster.getConnector();
    
    // get a security operations object as the root user
    SecurityOperations adminSecOps = adminConn.securityOperations();
    
    // admin creates a new user
    String principal = "testUser";
    PasswordToken token = new PasswordToken("password");
    adminSecOps.createLocalUser(principal, token);
    
    // get a connector as our new user
    Connector userConn = ExampleMiniCluster.getInstance().getConnector(principal, token);
    
    // -------- table permissions
    String adminTable = "adminTable";
    adminConn.tableOperations().create(adminTable);
    
    // user tries to write data
    BatchWriterConfig config = new BatchWriterConfig();
    BatchWriter writer = userConn.createBatchWriter(adminTable, config);
    Mutation m = new Mutation("testRow");
    m.put("", "testColumn", "testValue");
    
    try {    
      writer.addMutation(m);
      writer.close();
    } catch (Exception ex) {
      System.out.println("user unable to write to admin table");
    }
    
    // admin grants permission to write data to user
    adminSecOps.grantTablePermission(principal, adminTable, TablePermission.WRITE);
    
    writer = userConn.createBatchWriter(adminTable, config);
    
    writer.addMutation(m);
    writer.close();
    System.out.println("user can write to admin table");
    
    // -------- namespace permissions
    
    String adminNS = "adminNamespace";
    adminConn.namespaceOperations().create(adminNS);
    
    
    try {
      userConn.tableOperations().create(adminNS + ".userTable");
    } catch (AccumuloSecurityException ex) {
      System.out.println("user unauthorized to create table in root namespace");
    }
    
    // allow user to create tables in the root NS
    adminSecOps.grantNamespacePermission(principal, adminNS, NamespacePermission.CREATE_TABLE);
    
    userConn.tableOperations().create(adminNS + ".userTable");
    System.out.println("table creation in root namespace succeeded");
    
    
    // -------- system permissions
    String userNS = "userNamespace";
    
    try {
      userConn.namespaceOperations().create(userNS);
    } catch (AccumuloSecurityException ex) {
      System.out.println("user unauthorized to create namespace");
    }
    
    adminSecOps.grantSystemPermission(principal, SystemPermission.CREATE_NAMESPACE);
    userConn.namespaceOperations().create(userNS);
    System.out.println("create namespace succeeded");
    
    // users can create any table in a namespace they created
    userConn.tableOperations().create(userNS + ".userTable");
    System.out.println("table creation in user-owned namespace succeeded");
    
    // user tries to create user table in default namespace
    String userTable = "userTable";
    
    try {
      userConn.tableOperations().create(userTable);
    } catch (AccumuloSecurityException ex) {
      System.out.println("user unauthorized to create table in default namespace");
    }
    
    adminSecOps.grantSystemPermission(principal, SystemPermission.CREATE_TABLE);
    
    userConn.tableOperations().create(userTable);
    System.out.println("table creation in default namespace succeeded");
  }
}
