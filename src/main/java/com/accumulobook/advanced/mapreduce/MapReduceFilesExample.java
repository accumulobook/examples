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
import com.accumulobook.basic.WikipediaClient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public class MapReduceFilesExample implements Tool {
	
	public static void main(String[] args) throws Exception {
    
    // startup mini accumulo and ingest data
    System.out.println("Ingesting example wikipedia articles ...");
    Connector conn = ExampleMiniCluster.getConnector();
    
    WikipediaClient client = new WikipediaClient(conn, new Authorizations());
    
    client.ingest(WikipediaConstants.HADOOP_PAGES);
    
    // start mr cluster
    System.out.println("done. starting mini mr cluster ...");
    MiniMRClusterRunner mmrce = new MiniMRClusterRunner();
    mmrce.setup();
    mmrce.start();
    
    System.out.println("started. running mapreduce files example ...");
    MapReduceFilesExample mrfe = new MapReduceFilesExample();
    
    mrfe.run(new String[]{
      ExampleMiniCluster.getInstanceName(), 
      ExampleMiniCluster.getZooKeepers(),
      "root", "password"});
    
    System.out.println("done. Starting shell ...");
    
    String[] shellArgs = new String[]{"-u", "root", "-p", "password", "-z",
      ExampleMiniCluster.getInstanceName(), ExampleMiniCluster.getZooKeepers()};

    Shell.main(shellArgs);
    mmrce.stop();
    ExampleMiniCluster.shutdown();
  }

  Configuration conf = null;
  
  @Override
  public int run(String[] args) throws Exception {
    
    Job job = Job.getInstance(this.getConf());
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(WordCount.WordCountMapper.class);
    job.setCombinerClass(WordCount.WordCountCombiner.class); 
    job.setReducerClass(WordCount.WordCountReducer.class);

    // clone the articles table
    ZooKeeperInstance inst = new ZooKeeperInstance(args[0], args[1]);
    Connector conn = inst.getConnector(args[2], new PasswordToken(args[3]));
	
    conn.tableOperations().clone(
			WikipediaConstants.ARTICLES_TABLE, 
			WikipediaConstants.ARTICLES_TABLE_CLONE, 
			true, 
			Collections.EMPTY_MAP, 
			Collections.EMPTY_SET);
	
    // take cloned table offline, waiting until the operation is complete
    boolean wait = true;
    conn.tableOperations().offline(WikipediaConstants.ARTICLES_TABLE_CLONE, wait);
	
    ClientConfiguration zkiConfig = new ClientConfiguration()
            .withInstance(args[0])
            .withZkHosts(args[1]);
    
    // input
    job.setInputFormatClass(AccumuloInputFormat.class);
    AccumuloInputFormat.setInputTableName(job, WikipediaConstants.ARTICLES_TABLE_CLONE);
    List<Pair<Text,Text>> columns = new ArrayList<>();
    columns.add(new Pair(WikipediaConstants.CONTENTS_FAMILY_TEXT, new Text("")));
  
    AccumuloInputFormat.fetchColumns(job, columns);
    AccumuloInputFormat.setZooKeeperInstance(job, zkiConfig);
    AccumuloInputFormat.setConnectorInfo(job, args[2], new PasswordToken(args[3]));
	
    // configure to use underlying RFiles
    AccumuloInputFormat.setOfflineTableScan(job, true);
	
    // output
    job.setOutputFormatClass(AccumuloOutputFormat.class);

    BatchWriterConfig bwConfig = new BatchWriterConfig();
    
    AccumuloOutputFormat.setBatchWriterOptions(job, bwConfig);
    AccumuloOutputFormat.setZooKeeperInstance(job, zkiConfig);
    AccumuloOutputFormat.setConnectorInfo(job, args[2], new PasswordToken(args[3]));
    AccumuloOutputFormat.setDefaultTableName(job, WikipediaConstants.WORD_COUNT_TABLE);
    AccumuloOutputFormat.setCreateTables(job, true);
    
    job.setJarByClass(WordCount.class);

    job.waitForCompletion(true);
    //job.submit();
    
    return 0;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    if(this.conf == null) {
      this.conf = new Configuration();
    }
    
    return this.conf;
  }
}
