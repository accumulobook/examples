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

import com.accumulobook.ExampleMiniCluster;
import com.accumulobook.WikipediaConstants;
import com.accumulobook.basic.WikipediaClient.WikipediaEditException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

/**
 * 
 * Demonstrate the use of conditional mutations
 */
public class WikipediaEditExample {
  

  public static void main(String[] args) throws Exception {

    Connector conn = ExampleMiniCluster.getConnector();

    WikipediaClient client = new WikipediaClient(conn, new Authorizations());
    
    client.ingest(WikipediaConstants.HADOOP_PAGES);

    Map<String, String> hadoopArticle = client.getContentsAndRevision("Apache_Hadoop");

    String originalContents = hadoopArticle.get(WikipediaConstants.CONTENTS_FAMILY);
    String newContents = originalContents.toLowerCase();
    String lastRevision = hadoopArticle.get(WikipediaConstants.REVISION_QUAL);


    // apply our edit
    try {
      if (client.updateContent("Apache_Hadoop", lastRevision, newContents)) {
        System.out.println("edit of revision " + lastRevision + " succeeded.");
      } else {
        System.out.println("edit of revision " + lastRevision + " failed.");
      }
    } catch (WikipediaEditException ex) {
      Logger.getLogger(WikipediaEditExample.class.getName()).log(Level.SEVERE, null, ex);
    }

    // if we try again, we should fail
    try {
      if (client.updateContent("Apache_Hadoop",  lastRevision, newContents)) {
        System.out.println("second edit of revision " + lastRevision + " succeeded.");
      } else {
        System.out.println("second edit of revision " + lastRevision + " failed.");
      }
    } catch (WikipediaEditException ex) {
      Logger.getLogger(WikipediaEditExample.class.getName()).log(Level.SEVERE, null, ex);
    }

    // need to pull current revision again
    hadoopArticle = client.getContentsAndRevision("Apache_Hadoop");
    String nextRevision = hadoopArticle.get(WikipediaConstants.REVISION_QUAL);

    // put back original contents
    // now we should succeed
    try {
      if (client.updateContent("Apache_Hadoop", nextRevision, originalContents)) {
        System.out.println("edit of revision " + nextRevision + " succeeded.");
      } else {
        System.out.println("edit of revision " + nextRevision + " failed.");
      }
    } catch (WikipediaEditException ex) {
      Logger.getLogger(WikipediaEditExample.class.getName()).log(Level.SEVERE, null, ex);
    }
    
    System.exit(0);
  }
}
