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

import com.accumulobook.ExampleMiniCluster;
import com.accumulobook.WikipediaConstants;
import com.accumulobook.basic.WikipediaPagesFetcher;
import info.bliki.wiki.dump.IArticleFilter;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import info.bliki.wiki.dump.WikiXMLParser;
import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.hadoop.io.Text;
import org.xml.sax.SAXException;

public class WordCountIngester {

  private PlainTextConverter converter;
  private WikiModel model;
  
  private final BatchWriter batchWriter;
  private boolean closed = true;
  
  private final Connector conn;
 
  
  private class WArticleFilter implements IArticleFilter {

    @Override
    public void process(WikiArticle article, Siteinfo info)
            throws SAXException {

      System.out.println("Parsing " + article.getTitle());

      String wikitext = article.getText();
      String plaintext = model.render(converter, wikitext)
              .replace("{{", " ")
              .replace("}}", " ");

      // count words in article
      HashMap<String, Integer> wordCounts = new HashMap<>();
      for(String word : plaintext.replaceAll("^[a-zA-Z]"," ").toLowerCase().split("\\s+")) {
        if(!wordCounts.containsKey(word)) {
          wordCounts.put(word, 0);
        }
        
        wordCounts.put(word, wordCounts.get(word) + 1);
      }
      
      try {
        for (Map.Entry<String, Integer> e : wordCounts.entrySet()) {
          Mutation m = new Mutation(e.getKey());
          m.put("counts", "", e.getValue().toString());

          batchWriter.addMutation(m);
        }
      } catch (MutationsRejectedException e) {

        e.printStackTrace();
      }
    }
  }
  
  /**
   *
   * Download the Wikipedia pages specified by the pages parameter,
   * parse them, and ingest into the Accumulo table.
   * 
   * This uses Wikipedia's Special Export API: 
   * http://en.wikipedia.org/w/index.php?title=Special:Export
   * 
   * @param pages
   * @throws Exception
   */
  public void ingest(final String ... pages) throws Exception {
    
    IArticleFilter handler = new WArticleFilter();
    WikiXMLParser wxp = new WikiXMLParser(WikipediaPagesFetcher.fetch(pages), handler);

    _ingest(wxp, conn);
  }

  private void _ingest(WikiXMLParser wxp, Connector conn) 
          throws Exception {
    
    if (closed) {
      throw new IOException("client closed");
    }
    

    // setup the wikipedia parser
    converter = new PlainTextConverter(true);
    model = new WikiModel("", "");

    // parse the article, writing new mutations to Accumulo as we go

    System.out.println("Parsing articles ...");
    wxp.parse();

    System.out.println("done.");
  }
  
  public void close() throws MutationsRejectedException {
    batchWriter.close();
    closed = true;
  }
  
  public WordCountIngester(final Connector conn) throws TableNotFoundException, AccumuloException, AccumuloSecurityException, TableExistsException {
    this.conn = conn;
    
    if (!conn.tableOperations().exists(WikipediaConstants.WORD_COUNT_TABLE)) {
      conn.tableOperations().create(WikipediaConstants.WORD_COUNT_TABLE);

      // setup combiner
      IteratorSetting iterSet = new IteratorSetting(
              10,
              "summingCombiner",
              org.apache.accumulo.core.iterators.user.SummingCombiner.class.getName());
      
      SummingCombiner.setEncodingType(iterSet, SummingCombiner.Type.STRING);
      
      List<IteratorSetting.Column> columns = new ArrayList<>();
      columns.add(new IteratorSetting.Column(new Text("counts"), new Text("")));
      
      SummingCombiner.setColumns(iterSet, columns);
      conn.tableOperations().attachIterator(WikipediaConstants.WORD_COUNT_TABLE, iterSet);
    }
    
    BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxMemory(1000000L);
    config.setMaxWriteThreads(10);
    config.setMaxLatency(10, TimeUnit.SECONDS);
    
    batchWriter = conn.createBatchWriter(WikipediaConstants.WORD_COUNT_TABLE, config);
    closed = false;
  }
  
  public static void main(String[] args) throws Exception {
    Connector conn = ExampleMiniCluster.getConnector();
    
    WordCountIngester ingester = new WordCountIngester(conn);
    ingester.ingest(WikipediaConstants.HADOOP_PAGES);
    ingester.close();
    
    System.out.println("starting shell ...");
    String[] shellArgs = new String[]{"-u", "root", "-p", "password", "-z",
      ExampleMiniCluster.getInstanceName(), ExampleMiniCluster.getZooKeepers()};

    Shell.main(shellArgs);
  }
}
