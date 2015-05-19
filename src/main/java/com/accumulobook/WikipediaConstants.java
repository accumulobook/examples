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

import org.apache.hadoop.io.Text;

public class WikipediaConstants {

  public static final String ARTICLES_TABLE = "WikipediaArticles";
  public static final String ARTICLES_TABLE_CLONE = "WikipediaArticlesCloned";
  public static final String WORD_COUNT_TABLE = "WikipediaWordCount";
  public static final String INDEX_TABLE = "WikipediaIndex";
  public static final String DOC_PARTITIONED_TABLE = "WikipediaPartitioned";
  
  public static final String ARTICLE_CONTENTS_TOKEN = "contents";
  
  public static final String CONTENTS_FAMILY = "contents";
  public static final Text CONTENTS_FAMILY_TEXT = new Text(CONTENTS_FAMILY);
  public static final String METADATA_FAMILY = "metadata";
  public static final Text METADATA_FAMILY_TEXT = new Text(METADATA_FAMILY);
  public static final String INDEX_FAMILY = "index";
  public static final String NAMESPACE_QUAL = "namespace";
  public static final String TIMESTAMP_QUAL = "timestamp";
  public static final String ID_QUAL = "id";
  public static final String REVISION_QUAL = "revision";
  public static final Text REVISION_QUAL_TEXT = new Text(REVISION_QUAL);
  
  public static String[] HADOOP_PAGES = {
    "Cloudera", 
    "Apache_HBase", 
    "Apache_ZooKeeper", 
    "Apache_Hive", 
    "Apache_Mahout", 
    "MapR", 
    "Hortonworks", 
    "Apache_Accumulo", 
    "Sqoop", 
    "Apache_Hadoop", 
    "Oozie", 
    "Cloudera_Impala", 
    "Apache_Giraph", 
    "Apache_Spark"};
}
