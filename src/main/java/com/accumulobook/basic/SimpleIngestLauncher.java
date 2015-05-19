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
package com.accumulobook.basic;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.Connector;

import com.beust.jcommander.Parameter;
import org.apache.accumulo.core.security.Authorizations;

public class SimpleIngestLauncher {

	private static class Opts extends ClientOpts {
	    @Parameter(names="--input", required = true, description="input file")
	    String inputFile;
	}
	
	public static void main(String[] args) throws Exception {
		
		// parse options from command line
		Opts opts = new Opts();
		BatchWriterOpts bwOpts = new BatchWriterOpts();
		opts.parseArgs(SimpleIngestLauncher.class.getName(), args, bwOpts);
		
		// get a connector to Accumulo
		Connector conn = opts.getConnector();
		
		// call ingest 
        WikipediaClient client = new WikipediaClient(conn, new Authorizations());
		client.ingest(opts.inputFile);
	}
}
