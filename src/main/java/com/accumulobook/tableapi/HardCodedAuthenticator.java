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

import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Set;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.server.security.handler.Authenticator;
import org.apache.accumulo.server.security.handler.Authorizor;
import org.apache.accumulo.server.security.handler.PermissionHandler;

/**
 * 
 * Simple authenticator for example purposes only
 */
public class HardCodedAuthenticator implements Authenticator {
  
  @Override
  public void initialize(String instanceId, boolean initialize) {
    
  }

  @Override
  public boolean validSecurityHandlers(Authorizor auth, PermissionHandler pm) {
    return true;
  }

  @Override
  public void initializeSecurity(TCredentials credentials, String principal, byte[] token) throws AccumuloSecurityException, ThriftSecurityException {
    
  }

  @Override
  public boolean authenticateUser(String principal, AuthenticationToken token) throws AccumuloSecurityException {
    return principal.equals("onlyUser") && new String(((PasswordToken)token).getPassword()).equals("onlyPassword");
  }

  @Override
  public Set<String> listUsers() throws AccumuloSecurityException {
    HashSet<String> users = new HashSet<String>();
	users.add("onlyUser");
	return users;
  }

  @Override
  public void createUser(String principal, AuthenticationToken token) throws AccumuloSecurityException {
    
  }

  @Override
  public void dropUser(String user) throws AccumuloSecurityException {
    
  }

  @Override
  public void changePassword(String principal, AuthenticationToken token) throws AccumuloSecurityException {
    
  }

  @Override
  public boolean userExists(String user) throws AccumuloSecurityException {
    return user.equals("onlyUser");
  }

  @Override
  public Set<Class<? extends AuthenticationToken>> getSupportedTokenTypes() {
    return (Set)Sets.newHashSet(PasswordToken.class);
  }

  @Override
  public boolean validTokenClass(String tokenClass) {
    return tokenClass.equals(PasswordToken.class.toString());
  }
}
