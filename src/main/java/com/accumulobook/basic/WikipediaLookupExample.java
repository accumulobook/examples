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

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;

import com.accumulobook.ExampleMiniCluster;
import com.accumulobook.WikipediaConstants;

/**
 * Look up a Wikipedia article by title
 *
 */
public class WikipediaLookupExample {
  
  /**
   * For running from the command line
   *
   * @param args
   * @throws IOException
   * @throws InterruptedException
   * @throws AccumuloException
   * @throws AccumuloSecurityException
   * @throws TableNotFoundException
   */
  public static void main(String[] args) throws Exception {
    
    Connector conn = ExampleMiniCluster.getConnector();
	
    WikipediaClient client = new WikipediaClient(conn, new Authorizations());
    client.ingest(WikipediaConstants.HADOOP_PAGES);
    
    System.out.println("\nPrinting out one article:\n--------------");
    client.printArticle("Apache_Accumulo");
    
    System.out.println("\nPrinting out one column:\n--------------");
    client.scanColumn(WikipediaConstants.METADATA_FAMILY, WikipediaConstants.REVISION_QUAL);
	
	System.exit(0);
  }
}
