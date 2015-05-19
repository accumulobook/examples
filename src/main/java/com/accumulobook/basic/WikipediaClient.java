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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.transform;

import com.google.common.base.Function;
import com.accumulobook.WikipediaConstants;
import info.bliki.wiki.dump.IArticleFilter;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import info.bliki.wiki.dump.WikiXMLParser;
import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import static org.apache.accumulo.core.client.ConditionalWriter.Status.ACCEPTED;
import static org.apache.accumulo.core.client.ConditionalWriter.Status.INVISIBLE_VISIBILITY;
import static org.apache.accumulo.core.client.ConditionalWriter.Status.REJECTED;
import static org.apache.accumulo.core.client.ConditionalWriter.Status.UNKNOWN;
import static org.apache.accumulo.core.client.ConditionalWriter.Status.VIOLATED;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 * An example client showcasing usage of the Accumulo API
 * 
 */
public class WikipediaClient {
  
  public static class WikipediaEditException extends Exception {

    private WikipediaEditException(Exception ex) {
      super(ex);
    }

    private WikipediaEditException(String msg) {
      super(msg);
    }
  }
 
  private final Function<String, Range> rangeConverter = new Function<String,Range>() {
    @Override
    public Range apply(String f) {
      return new Range(f);
    }    
  };
  
  private static final Logger logger = LoggerFactory.getLogger(WikipediaClient.class);
  private PlainTextConverter converter;
  private WikiModel model;
  
  private final Authorizations auths;
  private  BatchWriter batchWriter;
  private  ConditionalWriter conditionalWriter;
  private boolean closed = true;
  
  private final Connector conn;

  public WikipediaClient(final Connector conn, final Authorizations auths) throws TableNotFoundException, AccumuloException, AccumuloSecurityException, TableExistsException {
    
    this.auths = auths;
    this.conn = conn;
    
	if(!conn.tableOperations().exists(WikipediaConstants.ARTICLES_TABLE))
	  conn.tableOperations().create(WikipediaConstants.ARTICLES_TABLE);
	
	if(!conn.securityOperations().getUserAuthorizations("root").contains(WikipediaConstants.ARTICLE_CONTENTS_TOKEN))
	  conn.securityOperations().changeUserAuthorizations("root", new Authorizations(WikipediaConstants.ARTICLE_CONTENTS_TOKEN));
	
    ConditionalWriterConfig condConfig = new ConditionalWriterConfig();
    condConfig.setMaxWriteThreads(10);

    conditionalWriter = conn.createConditionalWriter(WikipediaConstants.ARTICLES_TABLE, condConfig);
    
    BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxMemory(1000000L);
    config.setMaxWriteThreads(10);
    config.setMaxLatency(10, TimeUnit.SECONDS);
    batchWriter = conn.createBatchWriter(WikipediaConstants.ARTICLES_TABLE, config);
    closed = false;
  }
  
  /**
   * Delete a single article
   * 
   * @param title
   */
  public boolean deleteArticle(final String title) throws IOException {

    if (closed)
      throw new IOException("client closed");

    BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxMemory(1000000L);
    config.setMaxWriteThreads(10);
    config.setMaxLatency(10, TimeUnit.SECONDS);

    try {
      BatchDeleter deleter = conn.createBatchDeleter(WikipediaConstants.ARTICLES_TABLE, auths, 10, null);
      deleter.setRanges(Collections.singleton(new Range(title)));
      deleter.delete();
      deleter.close();
      
      return true;
    } catch (TableNotFoundException | MutationsRejectedException ex) {
      logger.error(ex.getMessage());
    }
    return false;
  }
  
  /**
   *
   * Delete a list of articles
   * 
   * @param titles
   * @return success
   * 
   * @throws java.io.IOException
   */
  public boolean deleteArticles(final String ... titles) throws IOException {
    
    if (closed)
      throw new IOException("client closed");

    BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxMemory(1000000L);
    config.setMaxWriteThreads(10);
    config.setMaxLatency(10, TimeUnit.SECONDS);
    int numThreads = 10;
    
    try {
      BatchDeleter deleter = conn.createBatchDeleter(
              WikipediaConstants.ARTICLES_TABLE, auths, numThreads, config);
      deleter.setRanges(transform(newArrayList(titles), rangeConverter));
      deleter.delete();
      deleter.close();
      
      return true;
    } catch (TableNotFoundException | MutationsRejectedException ex) {
      logger.error(ex.getMessage());      
    }
    return false;
  }
  
  /**
   * Write a new version of a page, but only if no one else has modified it
   * since we read it
   *
   * @param title
   * @param lastRevision
   * @param contents
   */
  public boolean updateContent(
          final String title,
          final String lastRevision,
          final String contents) throws WikipediaEditException, IOException {

    if (closed) 
      throw new IOException("client closed");

    final String newRevision = Integer.toString(Integer.parseInt(lastRevision) + 1);

    ConditionalMutation cm = new ConditionalMutation(title);
    Condition lastRevisionStillCurrent = new Condition(
            WikipediaConstants.METADATA_FAMILY,
            WikipediaConstants.REVISION_QUAL);

    // this requires that the version in the table is the last revision we read
    lastRevisionStillCurrent.setValue(lastRevision);
    cm.addCondition(lastRevisionStillCurrent);

    // add puts for our changes
    cm.put(WikipediaConstants.METADATA_FAMILY,
            WikipediaConstants.REVISION_QUAL,
            newRevision);

    cm.put(WikipediaConstants.CONTENTS_FAMILY, "", contents);

    // submit to the server
    ConditionalWriter.Result r = conditionalWriter.write(cm);
    try {
      switch (r.getStatus()) {
        case ACCEPTED:
          return true;
        case REJECTED:
          return false;
        case VIOLATED:
          throw new WikipediaEditException("constraint violated");
        case UNKNOWN: // could retry
          logger.warn("unknown error from server: {0}", r.getTabletServer());
          return false;
        case INVISIBLE_VISIBILITY:
          throw new WikipediaEditException("condition contained a visibility the user cannot satisfy");
        default:
          throw new AssertionError(r.getStatus().name());
      }
    } catch (AccumuloException | AccumuloSecurityException ex) {
      throw new WikipediaEditException(ex);
    }
  }

  /**
   *
   * @param title
   * @param attribute
   * @param value
   * @throws MutationsRejectedException
   */
  public void updateMetadata(
          final String title,
          final String attribute,
          final String value,
          final boolean flush) 
          throws MutationsRejectedException, IOException {
    
    if (closed) 
      throw new IOException("client closed");
    
    Mutation m = new Mutation(title);
    m.put(WikipediaConstants.METADATA_FAMILY, attribute, value);
    
    batchWriter.addMutation(m);
    
    if(flush)
      batchWriter.flush();
  }
  
  /**
   *
   * @param articleTitle
   * @throws TableNotFoundException
   */
  public void printArticle(String articleTitle) throws TableNotFoundException {

    Scanner scanner = conn.createScanner(WikipediaConstants.ARTICLES_TABLE, auths);
    // attempt to read one article
    scanner.setRange(new Range(articleTitle));

    for (Map.Entry<Key, Value> entry : scanner) {
      Key key = entry.getKey();
      String field;
      if (key.getColumnFamily().toString().equals("contents")) {
        field = "contents";
      } else {
        field = key.getColumnQualifier().toString();
      }

      String valueString = new String(entry.getValue().get());
      System.out.println(field + "\t" + valueString);
    }
  }
  
  /**
   *
   * @param columnFamily
   * @param columnQualifier
   * @throws TableNotFoundException
   */
  public void scanColumn(String columnFamily, String columnQualifier) 
          throws TableNotFoundException {
    
    Scanner scanner = conn.createScanner(WikipediaConstants.ARTICLES_TABLE, auths);
    
    // scan one column from all rows
    scanner.fetchColumn(new Text(columnFamily), new Text(columnQualifier));

    for (Map.Entry<Key, Value> entry : scanner) {
      Key key = entry.getKey();
      
      String valueString = new String(entry.getValue().get());
      System.out.println(key.getRow().toString() + "\t" + valueString);
    }
  }

  /**
   * 
   * Retrieve an article's contents and revision number.
   * We use isolation to ensure the content and revision number are kept
   * in sync.
   * 
   * @param articleTitle
   * @return
   * @throws TableNotFoundException
   * @throws IOException
   */
  public Map<String, String> getContentsAndRevision(String articleTitle) throws TableNotFoundException, IOException {

    Scanner scanner = conn.createScanner(WikipediaConstants.ARTICLES_TABLE, auths);

    // ensure we get synchronized revision and contents
    scanner.enableIsolation();

    scanner.setRange(Range.exact(articleTitle));
    scanner.fetchColumnFamily(WikipediaConstants.CONTENTS_FAMILY_TEXT);
    scanner.fetchColumn(WikipediaConstants.METADATA_FAMILY_TEXT, WikipediaConstants.REVISION_QUAL_TEXT);

    Map<String, String> result = new HashMap<>();

    for(Map.Entry<Key, Value> entry : scanner) {
      Key key = entry.getKey();
      String valueString = new String(entry.getValue().get(), "UTF-8");
      
      if (key.getColumnFamily().toString().equals(WikipediaConstants.CONTENTS_FAMILY)) 
        result.put(WikipediaConstants.CONTENTS_FAMILY, valueString);
       
      if(key.getColumnFamily().toString().equals(WikipediaConstants.METADATA_FAMILY))
        result.put(WikipediaConstants.REVISION_QUAL, valueString);
    }
  
    return result;
  }
  
  
  /**
   *
   * @throws MutationsRejectedException
   */
  public void close() throws MutationsRejectedException {
    conditionalWriter.close();
    batchWriter.close();
    closed = true;
  }
  

  private class WArticleFilter implements IArticleFilter {

    @Override
    public void process(WikiArticle article, Siteinfo info)
            throws SAXException {

      System.out.println("Parsing " + article.getTitle());

      String wikitext = article.getText();
      String plaintext = model.render(converter, wikitext)
              .replace("{{", " ")
              .replace("}}", " ");

      Mutation m = new Mutation(article.getTitle().replace(" ", "_"));

      m.put(WikipediaConstants.CONTENTS_FAMILY, "", plaintext);
      m.put(WikipediaConstants.METADATA_FAMILY, WikipediaConstants.NAMESPACE_QUAL, article.getNamespace());
      m.put(WikipediaConstants.METADATA_FAMILY, WikipediaConstants.TIMESTAMP_QUAL, article.getTimeStamp());
      m.put(WikipediaConstants.METADATA_FAMILY, WikipediaConstants.ID_QUAL, article.getId());
      m.put(WikipediaConstants.METADATA_FAMILY, WikipediaConstants.REVISION_QUAL, article.getRevisionId());


      try {
        batchWriter.addMutation(m);
      } catch (MutationsRejectedException e) {
        logger.error(e.getMessage());
      }
    }
  }

  /**
   *
   * Parse all Wikipedia articles found in the file
   * specified by filename, and ingest them into the Accumulo table.
   * 
   * @param filename
   * @throws Exception
   */
  public void ingest(final String filename) throws Exception {
    
    IArticleFilter handler = new WArticleFilter();
    WikiXMLParser wxp = new WikiXMLParser(filename, handler);

    _ingest(wxp, conn);
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
    
    if (closed) 
      throw new IOException("client closed");

    // setup the wikipedia parser
    converter = new PlainTextConverter(true);
    model = new WikiModel("", "");

    // parse the article, writing new mutations to Accumulo as we go

    System.out.println("Parsing articles ...");
    wxp.parse();

	batchWriter.flush();
    System.out.println("done.");
  }
  
  public void setupLocalityGroups(final boolean compact) throws 
          AccumuloException, 
          AccumuloSecurityException, 
          TableNotFoundException {
    
    Set<Text> contentGroup = new HashSet<>();
    contentGroup.add(WikipediaConstants.CONTENTS_FAMILY_TEXT);
    
    Set<Text> metadataGroup = new HashSet<>();
    metadataGroup.add(WikipediaConstants.METADATA_FAMILY_TEXT);
    
    Map<String, Set<Text>> groups = new HashMap<>();
    groups.put("contentGroup", contentGroup);
    groups.put("metadataGroup", metadataGroup);
    
    conn.tableOperations().setLocalityGroups(WikipediaConstants.ARTICLES_TABLE, groups);
    
    if(compact) {
      conn.tableOperations().compact(
              WikipediaConstants.ARTICLES_TABLE,
              null,
              null,
              false,
              false);
    } 
  }
}
