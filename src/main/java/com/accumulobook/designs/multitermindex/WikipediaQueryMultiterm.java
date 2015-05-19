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

import com.accumulobook.designs.termindex.WikipediaQuery;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.IndexedDocIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;
import com.accumulobook.WikipediaConstants;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Range;

/**
 *
 */
public class WikipediaQueryMultiterm {

  public static class Opts extends ClientOpts {

    @Parameter(names = "--terms", required = true, description = "comma-separated terms to search wikipedia articles for")
    String terms;
  }

  private final Connector conn;
  private final Authorizations auths;

  public WikipediaQueryMultiterm(Connector connector) {
    conn = connector;
    auths = new Authorizations(WikipediaConstants.ARTICLE_CONTENTS_TOKEN);
  }
  
  /*
   * row: shardID, colfam: indexColf, colqual: doctype\0docID\0info, value: doc 
   */
  public void printResults(String[] terms) throws TableNotFoundException {
    
    BatchScanner scanner = conn.createBatchScanner(WikipediaConstants.DOC_PARTITIONED_TABLE, auths, 10);
    scanner.setTimeout(1, TimeUnit.MINUTES);
    scanner.setRanges(Collections.singleton(new Range()));
    
    Text[] termTexts = new Text[terms.length];
    for (int i = 0; i < terms.length; i++) {
      termTexts[i] = new Text(terms[i]);
    }

    // lookup all articles containing the terms
    IteratorSetting is = new IteratorSetting(50, IndexedDocIterator.class);
    IndexedDocIterator.setColfs(is, "ind", "doc");
    IndexedDocIterator.setColumnFamilies(is, termTexts);
    scanner.addScanIterator(is);

    for (Entry<Key, Value> entry : scanner) {
      String[] parts = entry.getKey().getColumnQualifier().toString().split("\0");
      System.out.println(
              "doctype: " + parts[0] + 
              "\ndocID:" + parts[1] +
              "\ninfo: " + parts[2] +
              "\n\ntext: " + entry.getValue().toString());
    }
  }

  public static void main(String[] args) throws Exception {

    Opts opts = new Opts();
    ScannerOpts sOpts = new ScannerOpts();

    opts.parseArgs(WikipediaQuery.class.getName(), args, sOpts);
    Connector conn = opts.getConnector();

    new WikipediaQueryMultiterm(conn).printResults(opts.terms.split(","));

  }
}
