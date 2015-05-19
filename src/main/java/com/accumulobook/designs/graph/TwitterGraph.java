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
package com.accumulobook.designs.graph;

import com.google.common.base.Optional;
import com.accumulobook.ExampleMiniCluster;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.security.Authorizations;
import org.ubiety.ubigraph.UbigraphClient;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.UserMentionEntity;
import twitter4j.auth.AccessToken;

public class TwitterGraph {

  public final static String TWITTER_GRAPH_TABLE = "twitterGraph";
  
  
  private static class TGStatusListener implements StatusListener {
    
    private final BatchWriter writer;

    public TGStatusListener(final BatchWriter writer) {
      this.writer = writer;
    }
    
    @Override
    public void onStatus(Status status) {
      
      String user = status.getUser().getScreenName();
      
      if(status.getInReplyToScreenName() != null) {
        // keep track of replies
        Graph.writeEdge(user, status.getInReplyToScreenName(), "reply", 1, writer, true);
      }
      
      // track mentions
      for(UserMentionEntity mention : status.getUserMentionEntities()) {
        Graph.writeEdge(user, mention.getScreenName(), "mention", 1, writer, true);
      }
      
      // treat hashtags as nodes
      for(HashtagEntity ht: status.getHashtagEntities()) {
        Graph.writeEdge(user, ht.getText(), "hashtag", 1, writer, true);
      }
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice sdn) {
      // could fetch status and delete edge contributions
    }

    @Override
    public void onTrackLimitationNotice(int i) {
      System.err.println("got track limitation notice");
    }

    @Override
    public void onScrubGeo(long l, long l1) {
      // ignore
    }

    @Override
    public void onStallWarning(StallWarning sw) {
      // ignore
    }

    @Override
    public void onException(Exception excptn) {
      // ignore
    }
    
  }
  
  public static void ingest(
          final String[] args, 
          final BatchWriter writer,
          int seconds) throws Exception {
  
    String token = args[0];
    String tokenSecret = args[1];
    String consumerKey = args[2];
    String consumerSecret = args[3];
    
    TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.addListener(new TGStatusListener(writer));

		AccessToken accessToken = new AccessToken(token, tokenSecret);

		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		twitterStream.setOAuthAccessToken(accessToken);
		
    System.out.println("processing twitter stream ...");
		twitterStream.sample();
    
    Thread.sleep(seconds * 1000);
    
    System.out.println("shutting down twitter stream.");
    twitterStream.shutdown();
    
    writer.flush();
  }
  
  public static void main(String[] args) throws Exception {
    
    Connector conn = ExampleMiniCluster.getConnector();
    
    // create our table and get some data
    BatchWriter writer = Graph.setupTable(conn, TWITTER_GRAPH_TABLE, true);
    
    int seconds = 30;
    ingest(args, writer, seconds);
    
    // visualize our graph, one node at a time
    System.out.println("visualizing graph ...");
    UbigraphClient client = new UbigraphClient();
    
    
    
    for (String startNode : new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i"}) {


      Scanner startNodeScanner = conn.createScanner(TWITTER_GRAPH_TABLE, Authorizations.EMPTY);
      Optional<String> node = Graph.discoverNode(startNode, startNodeScanner);
      startNodeScanner.close();
      
      if (node.isPresent()) {

        // visualize start node
        int nodeId = client.newVertex();
        client.setVertexAttribute(nodeId, "label", node.get());

        Scanner neighborScanner = conn.createScanner(TWITTER_GRAPH_TABLE, Authorizations.EMPTY);
        for (String neighbor : Graph.getNeighbors(node.get(), neighborScanner, "ALL")) {

          // visualize neighbor node
          int neighborId = client.newVertex();
          client.setVertexAttribute(neighborId, "label", neighbor);

          // visualize edge
          client.newEdge(nodeId, neighborId);
        }
        neighborScanner.close();
      }
    }
  }
}
