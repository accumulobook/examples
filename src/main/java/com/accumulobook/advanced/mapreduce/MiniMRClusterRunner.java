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
package com.accumulobook.advanced.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;


public class MiniMRClusterRunner {
  
  private MiniMRClientCluster cluster;
  
  public void setup() throws IOException {
    Configuration conf = new YarnConfiguration();
    cluster = MiniMRClientClusterFactory.create(this.getClass(), 2, conf);
  }
  public static void main(String[] args) throws IOException {
    
    MiniMRClusterRunner m = new MiniMRClusterRunner();
    m.setup();
    
    m.start();
  }

  public void start() throws IOException {
    cluster.start();
  }
  
  public void stop() throws IOException {
    cluster.stop();
  }
}
