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
package com.accumulobook.designs.multitermindex;

import com.accumulobook.ExampleMiniCluster;
import info.bliki.wiki.dump.IArticleFilter;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import info.bliki.wiki.dump.WikiXMLParser;
import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;

import java.util.HashSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.xml.sax.SAXException;

import com.google.common.collect.Sets;
import com.accumulobook.WikipediaConstants;
import com.accumulobook.basic.WikipediaPagesFetcher;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.shell.Shell;

/**
 * row: shardID, colfam: "", colqual: docID, value: doc
 *
 * row: shardID, colfam: indexColf\0term, colqual: docID, value: (empty)
 */
public class WikipediaIngestWithMultiTermIndexExample {

  private static PlainTextConverter converter;
  private static WikiModel model;
  private static BatchWriter writer;
  private static final int NUM_PARTITIONS = 10;

  public static class WArticleFilter implements IArticleFilter {

    private static final Value BLANK_VALUE = new Value("".getBytes());

    /*
     * row: shardID, colfam: docColf\0doctype, colqual: docID, value: doc
     * 
     * row: shardID, colfam: indexColf, colqual: term\0doctype\0docID\0info, value: (empty)
     * 
     */
    @Override
    public void process(WikiArticle article, Siteinfo info) throws SAXException {

      String wikitext = article.getText();
      String plaintext = model.render(converter, wikitext);
      plaintext = plaintext.replace("{{", " ").replace("}}", " ");

      Mutation m = new Mutation(Integer.toString(Math.abs(article.getTitle().hashCode()) % NUM_PARTITIONS));
      m.put("doc" + '\0' + "wikiDoc", article.getTitle(), plaintext);      

      // tokenize article contents on whitespace and punctuation and set to lowercase
      HashSet<String> tokens = Sets.newHashSet(plaintext.toLowerCase().split("\\s+"));
      for (String token : tokens) {
        m.put("ind", token + '\0' + "wikiDoc" + '\0' + article.getTitle() + '\0', BLANK_VALUE);
      }

      try {
        writer.addMutation(m);
      } catch (MutationsRejectedException e) {
        throw new SAXException(e);
      }
    }
  }

  public static void ingest(Connector conn, String... pages) throws Exception {
    IArticleFilter handler = new WikipediaIngestWithMultiTermIndexExample.WArticleFilter();
    WikiXMLParser wxp = new WikiXMLParser(WikipediaPagesFetcher.fetch(pages), handler);

    _ingest(conn, wxp);
  }

  public static void _ingest(Connector conn, WikiXMLParser wxp) throws Exception {

    if (!conn.tableOperations().exists(WikipediaConstants.DOC_PARTITIONED_TABLE)) {
      conn.tableOperations().create(WikipediaConstants.DOC_PARTITIONED_TABLE);
      conn.securityOperations().changeUserAuthorizations("root", new Authorizations(WikipediaConstants.ARTICLE_CONTENTS_TOKEN));
    }

    // setup the wikipedia parser
    converter = new PlainTextConverter(true);
    model = new WikiModel("", "");

    BatchWriterConfig conf = new BatchWriterConfig();
    writer = conn.createBatchWriter(WikipediaConstants.DOC_PARTITIONED_TABLE, conf);

    System.out.println("Parsing articles and indexing ...");
    wxp.parse();

    writer.close();
    System.out.println("done.");
  }

  public static void main(String[] args) throws Exception {

    Connector conn = ExampleMiniCluster.getConnector();
    
    WikipediaIngestWithMultiTermIndexExample.ingest(conn, WikipediaConstants.HADOOP_PAGES);
    
    // uncomment to examine the partitioned table structure
    
    //String[] shellArgs = new String[]{"-u", "root", "-p", "password", "-z",
      //ExampleMiniCluster.getInstanceName(), ExampleMiniCluster.getZooKeepers()};

    //Shell.main(shellArgs);
    new WikipediaQueryMultiterm(conn).printResults(new String[]{"scalable","secure"});
  }
}
