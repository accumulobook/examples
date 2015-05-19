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

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import com.google.common.io.Files;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

public class ExampleMiniCluster {

	private static Instance instance;
	private static MiniAccumuloCluster accumulo;
	private static File tempDirectory;

		
	public static void shutdown() throws IOException, InterruptedException {
		if(accumulo != null) {
			accumulo.stop();
		}
		accumulo = null;
		tempDirectory.delete();
	}
	
	public static Instance getInstance() throws IOException, InterruptedException {
		if(instance == null) {
	
			tempDirectory = Files.createTempDir();
			accumulo = new MiniAccumuloCluster(tempDirectory, "password");
			accumulo.start();
			instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
		}
		return instance;
	}
	
	public static Connector getConnector() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
		return getInstance().getConnector("root", new PasswordToken("password"));
	}
	
	public static String getInstanceName() throws IOException {
		if(instance == null)
			throw new IOException("mini cluster is not yet running");
		
		return accumulo.getInstanceName();
	}
	
	public static String getZooKeepers() throws IOException {
		if(instance == null)
			throw new IOException("mini cluster is not yet running");
		
		return accumulo.getZooKeepers();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		ExampleMiniCluster.getInstance();
		
		System.out.println("MiniAccumuloCluster running with settings:");
		System.out.println("instance name: " + ExampleMiniCluster.accumulo.getInstanceName());
		System.out.println("zookeepers: " + ExampleMiniCluster.accumulo.getZooKeepers());
		System.out.println("user: root\npassword: password");
		System.out.println("hit any key to stop mini cluster.");
		System.in.read();
	}
}
