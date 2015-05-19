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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

public class WordCount implements Tool {

  private Configuration conf = null;
  
  @Override
  public int run(String[] args) throws Exception {
    
    Job job = Job.getInstance(new Configuration());
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(WordCountMapper.class);
    job.setCombinerClass(WordCountCombiner.class); 
    job.setReducerClass(WordCountReducer.class);

    // input
    job.setInputFormatClass(AccumuloInputFormat.class);

    ClientConfiguration zkiConfig = new ClientConfiguration()
            .withInstance(args[0])
            .withZkHosts(args[1]);
    
    AccumuloInputFormat.setInputTableName(job, WikipediaConstants.ARTICLES_TABLE);
    List<Pair<Text,Text>> columns = new ArrayList<>();
    columns.add(new Pair(WikipediaConstants.CONTENTS_FAMILY_TEXT, new Text("")));
  
    AccumuloInputFormat.fetchColumns(job, columns);
    AccumuloInputFormat.setZooKeeperInstance(job, zkiConfig);
    AccumuloInputFormat.setConnectorInfo(job, args[2], new PasswordToken(args[3]));
    
    // output
    job.setOutputFormatClass(AccumuloOutputFormat.class);

    BatchWriterConfig config = new BatchWriterConfig();
    
    AccumuloOutputFormat.setBatchWriterOptions(job, config);
    AccumuloOutputFormat.setZooKeeperInstance(job, zkiConfig);
    AccumuloOutputFormat.setConnectorInfo(job, args[2], new PasswordToken(args[3]));
    AccumuloOutputFormat.setDefaultTableName(job, WikipediaConstants.WORD_COUNT_TABLE);
    AccumuloOutputFormat.setCreateTables(job, true);
    
    job.setJarByClass(WordCount.class);

    job.submit();
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

  public static class WordCountMapper extends Mapper<Key,Value,Text,IntWritable> {
  
    @Override
    public void map(Key k, Value v, Context context) throws IOException, InterruptedException {
      
      String text = new String(v.get());
      
      // count words in article
      HashMap<String, Integer> wordCounts = new HashMap<>();
      for (String word : text.replaceAll("[^a-zA-Z ]", " ").toLowerCase().split("\\s+")) {
        if (!wordCounts.containsKey(word)) {
          wordCounts.put(word, 0);
        }
        wordCounts.put(word, wordCounts.get(word) + 1);
      }

      for (Map.Entry<String, Integer> e : wordCounts.entrySet()) {
        context.write(new Text(e.getKey()), new IntWritable(e.getValue()));
      }
    }
  }
  
  public static class WordCountCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
    
    @Override
    public void reduce(Text k, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for(IntWritable v : values) {
        sum += v.get();
      }
      
      context.write(k, new IntWritable(sum));
    }
  }
  
  public static class WordCountReducer extends Reducer<Text,IntWritable,Text,Mutation> {
    
    @Override
    public void reduce(Text k, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for(IntWritable v : values) {
        sum += v.get();
      }
      
      Mutation m = new Mutation(k.toString());
      m.put("count", "", Integer.toString(sum));
      
      context.write(null, m);
    }
  }
  
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
    WordCount wc = new WordCount();
    
    wc.run(new String[]{
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
}
