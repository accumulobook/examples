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
package com.accumulobook;

import java.io.IOException;
import org.apache.accumulo.proxy.Proxy;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.accumulo.proxy.ProxyServer;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;

public class ExampleMiniThriftProxy {

  public static void main(String[] args) throws Exception {

    Properties props = new Properties();
    ExampleMiniCluster.getInstance();

    props.setProperty("instance", ExampleMiniCluster.getInstanceName());
    props.setProperty("zookeepers", ExampleMiniCluster.getZooKeepers());

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void start() {
        try {
          ExampleMiniCluster.shutdown();
        } catch (IOException | InterruptedException ex) {
          Logger.getLogger(ExampleMiniThriftProxy.class.getName()).log(Level.SEVERE, null, ex);
        }
      }
    });

    TServer server = Proxy.createProxyServer(AccumuloProxy.class, ProxyServer.class, 42424, TCompactProtocol.Factory.class, props);
    server.serve();
  }
}
