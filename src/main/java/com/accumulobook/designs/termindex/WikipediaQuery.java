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
package com.accumulobook.designs.termindex;

import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.accumulobook.WikipediaConstants;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.lexicoder.DateLexicoder;

public class WikipediaQuery {

  public static class Opts extends ClientOpts {

    @Parameter(names = "--term", required = true, description = "term to search wikipedia articles for")
    String term;
  }
  
  private Connector conn;
  private Authorizations auths;

  public WikipediaQuery(Connector conn) {
    this.conn = conn;
    this.auths = new Authorizations(WikipediaConstants.ARTICLE_CONTENTS_TOKEN);
  }

  public void querySingleTerm(String term) throws TableNotFoundException {

    Scanner scanner = conn.createScanner(WikipediaConstants.INDEX_TABLE, auths);
    // lookup term in index
    scanner.setRange(Range.exact(term));
    
    // store all article titles returned
    HashSet<Range> matches = new HashSet<>();
    for (Entry<Key, Value> entry : scanner) {
      matches.add(new Range(entry.getKey().getColumnQualifier().toString()));
    }

    if(matches.isEmpty()) {
      System.out.println("no results");
      return;
    }
    
    for (Entry<Key, Value> entry : retrieveRecords(conn, matches)) {
      System.out.println("Title:\t" + entry.getKey().getRow().toString()
              + "\nRevision:\t" + entry.getValue().toString() + "\n");
    }
  }
  
  private Iterable<Entry<Key,Value>> retrieveRecords(Connector conn, Collection<Range> matches) throws TableNotFoundException {
    // retrieve original articles
    BatchScanner bscanner = conn.createBatchScanner(WikipediaConstants.ARTICLES_TABLE, auths, 10);
    bscanner.setRanges(matches);

    // fetch only the article contents
    bscanner.fetchColumn(new Text(WikipediaConstants.METADATA_FAMILY),
            new Text(WikipediaConstants.REVISION_QUAL));

    return bscanner;
  }
  
  // returns records matching any term
  public void queryMultipleTerms(String ... terms) throws TableNotFoundException {
    
    HashSet<String> matchingRecordIDs = new HashSet<>();
    
    for(String term : terms) {
      
      Scanner scanner = conn.createScanner(WikipediaConstants.INDEX_TABLE, auths);
      // lookup term in index
      scanner.setRange(Range.exact(term));
    
      for (Entry<Key, Value> entry : scanner) {
        matchingRecordIDs.add(entry.getKey().getColumnQualifier().toString());
      }
    }
    
    if(matchingRecordIDs.isEmpty()) {
      System.out.println("no results");
      return;
    }
    
    // convert to Ranges
    List<Range> ranges = Lists.newArrayList(
            Iterables.transform(matchingRecordIDs, new StringToRange()));
            
    for (Entry<Key, Value> entry : retrieveRecords(conn, ranges)) {
      System.out.println("Title:\t" + entry.getKey().getRow().toString()
              + "\nRevision:\t" + entry.getValue().toString() + "\n");
    }
  }
  
  final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
  
  public static String bytesToHex(byte[] bytes) {
    char[] hexChars = new char[bytes.length * 2];
    for ( int j = 0; j < bytes.length; j++ ) {
        int v = bytes[j] & 0xFF;
        hexChars[j * 2] = hexArray[v >>> 4];
        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
    }
    return new String(hexChars);
  }

  public void queryDateRange(
          final Date start, 
          final Date stop) throws TableNotFoundException {
    
    DateLexicoder dl = new DateLexicoder();
    
    Scanner scanner = conn.createScanner(WikipediaConstants.INDEX_TABLE, auths);
    
    // scan over the range of dates specified
    scanner.setRange(
            new Range(
              new Text(dl.encode(start)), 
              new Text(dl.encode(stop))));
    
    // store all article titles returned
    HashSet<Range> matches = new HashSet<>();
    for (Entry<Key, Value> entry : scanner) {
      matches.add(new Range(entry.getKey().getColumnQualifier().toString()));
    }

    if(matches.isEmpty()) {
      System.out.println("no results");
      return;
    }
    
    for (Entry<Key, Value> entry : retrieveRecords(conn, matches)) {
      System.out.println("Title:\t" + entry.getKey().getRow().toString()
              + "\nRevision:\t" + entry.getValue().toString() + "\n");
    }
  }
  
  private class StringToRange implements Function<String,Range> {

    @Override
    public Range apply(String f) {
      return new Range(f);
    }
  }
  
  // for querying an existing table
  public static void main(String[] args) throws Exception {
		
		// parse options from command line
		Opts opts = new Opts();
		BatchWriterOpts bwOpts = new BatchWriterOpts();
		opts.parseArgs(WikipediaQuery.class.getName(), args, bwOpts);
		
		// get a connector to Accumulo
		Connector connector = opts.getConnector();
		
		WikipediaQuery client = new WikipediaQuery(connector);
    client.querySingleTerm(opts.term);
	}
}
