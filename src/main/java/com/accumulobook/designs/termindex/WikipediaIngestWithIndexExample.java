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
import org.apache.accumulo.core.security.Authorizations;
import org.xml.sax.SAXException;

import com.google.common.collect.Sets;
import com.accumulobook.ExampleMiniCluster;
import com.accumulobook.WikipediaConstants;
import com.accumulobook.basic.WikipediaPagesFetcher;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.accumulo.core.client.lexicoder.DateLexicoder;


public class WikipediaIngestWithIndexExample {

	private static PlainTextConverter converter;
	private static WikiModel model;
	private static BatchWriter writer;
	private static BatchWriter indexWriter;
	
	public static class WArticleFilter implements IArticleFilter {

		private static final Value BLANK_VALUE = new Value("".getBytes());

    // 2014-04-17T16:12:43Z
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final DateLexicoder dateLexicoder = new DateLexicoder();
    
		@Override
		public void process(WikiArticle page, Siteinfo info)
				throws SAXException {

			String wikitext = page.getText();
			String plaintext = model.render(converter, wikitext);
			plaintext = plaintext
					.replace("{{",  " ")
					.replace("}}", " ");
			
      try {
        Mutation m = new Mutation(page.getTitle());
        m.put(WikipediaConstants.CONTENTS_FAMILY, "", plaintext);
        m.put(WikipediaConstants.METADATA_FAMILY, WikipediaConstants.NAMESPACE_QUAL, page.getNamespace());
        m.put(WikipediaConstants.METADATA_FAMILY, WikipediaConstants.TIMESTAMP_QUAL, page.getTimeStamp());
        m.put(WikipediaConstants.METADATA_FAMILY, WikipediaConstants.ID_QUAL, page.getId());
        m.put(WikipediaConstants.METADATA_FAMILY, WikipediaConstants.REVISION_QUAL, page.getRevisionId());

        writer.addMutation(m);

        // write index entries as well

        // tokenize article contents on whitespace and punctuation and set to lowercase
        HashSet<String> tokens = Sets.newHashSet(plaintext.replace("\"", "").toLowerCase().split("\\s+"));
        for (String token : tokens) {
          if (token.length() < 2) {  // skip single letters
            continue;
          }  
          
          Mutation indexMutation = new Mutation(token);
          indexMutation.put(WikipediaConstants.CONTENTS_FAMILY, page.getTitle(), BLANK_VALUE);

          indexWriter.addMutation(indexMutation);
        }

        try {
          Date d = dateFormat.parse(page.getTimeStamp());
          
          byte[] dateBytes = dateLexicoder.encode(d);
          
          Mutation dateIndexMutation = new Mutation(dateBytes);
          dateIndexMutation.put(WikipediaConstants.TIMESTAMP_QUAL, page.getTitle(), BLANK_VALUE);
          indexWriter.addMutation(dateIndexMutation);

        } catch (ParseException ex) {
          Logger.getLogger(WikipediaIngestWithIndexExample.class.getName()).log(Level.SEVERE, null, ex);
        }

      } catch (MutationsRejectedException e) {
        e.printStackTrace();
      }
    }
	}

	
  public static void ingest(Connector conn, String ... pages) throws Exception {
    IArticleFilter handler = new WArticleFilter();
    WikiXMLParser wxp = new WikiXMLParser(WikipediaPagesFetcher.fetch(pages), handler);
    
    _ingest(conn, wxp);
  }
  
  public static void ingest(Connector conn, String filename) throws Exception {
    IArticleFilter handler = new WArticleFilter();
    WikiXMLParser wxp = new WikiXMLParser(WikipediaPagesFetcher.fetch(filename), handler);
    
    _ingest(conn, wxp);
  }
  
	public static void _ingest(Connector conn, WikiXMLParser wxp) {
    
		try {
	
			if(!conn.tableOperations().exists(WikipediaConstants.ARTICLES_TABLE)) {
				conn.tableOperations().create(WikipediaConstants.ARTICLES_TABLE);
				conn.securityOperations().changeUserAuthorizations("root", new Authorizations(WikipediaConstants.ARTICLE_CONTENTS_TOKEN));
			}
			if(!conn.tableOperations().exists(WikipediaConstants.INDEX_TABLE)) {
				conn.tableOperations().create(WikipediaConstants.INDEX_TABLE);
				conn.securityOperations().changeUserAuthorizations("root", new Authorizations(WikipediaConstants.ARTICLE_CONTENTS_TOKEN));
			}
			
			// setup the wikipedia parser
			converter = new PlainTextConverter(true);
			model = new WikiModel("","");
						
			BatchWriterConfig conf = new BatchWriterConfig();
			writer = conn.createBatchWriter(WikipediaConstants.ARTICLES_TABLE, conf);
			indexWriter = conn.createBatchWriter(WikipediaConstants.INDEX_TABLE, conf);
		
			System.out.println("Parsing articles and indexing ...");
			wxp.parse();

			writer.close();
			indexWriter.close();
			System.out.println("done.");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
  
  // for testing using mini cluster
  public static void main(String[] args) throws Exception {
    
    Connector conn = ExampleMiniCluster.getConnector();
    
    WikipediaIngestWithIndexExample.ingest(conn, WikipediaConstants.HADOOP_PAGES);

    
    WikipediaQuery query = new WikipediaQuery(conn);
    System.out.println("querying for articles containing the term 'accumulo'");
    query.querySingleTerm("accumulo");
    
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    System.out.println("querying for articles from 2015-01-01 to 2016-01-01");
    query.queryDateRange(df.parse("2015-01-01"), df.parse("2016-01-01"));
  }
}
