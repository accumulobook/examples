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
package com.accumulobook.designs.rdf;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.accumulobook.ExampleMiniCluster;
import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.json.JSONArray;
import org.json.JSONObject;

public class FreebaseExample {

  private static final Value BLANK = new Value(new byte[0]);
  
  private static final Function<Entry<Key,Value>,String> cqSelector = 
          new Function<Entry<Key,Value>,String>() {

    @Override
    public String apply(Entry<Key, Value> e) {
      return e.getKey().getColumnQualifier().toString();
    }    
  };
  
  private static final Function<Entry<Key,Value>,String> cfqSelector = 
          new Function<Entry<Key,Value>,String>() {

    @Override
    public String apply(Entry<Key, Value> e) {
      return e.getKey().getColumnFamily().toString() + "\t" 
              + e.getKey().getColumnQualifier().toString();
    }    
  };
  
  public static List<Pair<String,String>> getProperties(
          final String subject) throws Exception {
    
    ArrayList<Pair<String,String>> properties = new ArrayList<>();
    
    URL url = new URL("https://www.googleapis.com/freebase/v1/topic/en/" + subject);
    
    StringWriter writer = new StringWriter();
    IOUtils.copy(url.openStream(), writer);

    String string = writer.toString();
    
    JSONObject obj = new JSONObject(string);
    
    JSONObject propArray = obj.getJSONObject("property");
    
    Iterator iter = propArray.keys();
    while(iter.hasNext()) {
      String propertyName = (String) iter.next();
      JSONObject property = propArray.getJSONObject(propertyName);
      
      JSONArray values = property.getJSONArray("values");
      for(int i=0; i < values.length(); i++) {
        properties.add(
              new Pair(propertyName, 
              values.getJSONObject(i).getString("text")));
      }
    }
    
    return properties;
  }
  
  public static void insertTriples(
          final String subject,
          final BatchWriter writer) throws Exception {
  
    final List<Pair<String,String>> properties = getProperties(subject);
    
    // for SPO triple
    Mutation spo = new Mutation("spo_" + subject);
    
    for(Pair<String, String> prop : properties) {
      
      String predicate = prop.getFirst();
      String object = prop.getSecond();
      
      spo.put(predicate, object, BLANK);
      
      // POS
      Mutation pos = new Mutation("pos_" + predicate);
      pos.put(object, subject, BLANK);
      writer.addMutation(pos);
      
      // OSP
      Mutation osp = new Mutation("osp_" + object);
      osp.put(subject, predicate, BLANK);
      writer.addMutation(osp);
    }
    
    writer.addMutation(spo);
    
    writer.flush();
  }
  
  public static Iterable<String> query(
          final String subject,
          final String predicate,
          final String object,
          final Scanner scanner) throws Exception {
    
    if(subject == null && predicate == null && object == null)
      throw new IllegalArgumentException("Must specify at least one of subject, predicate, object");
    
    if(subject != null) {
      if(predicate != null) { // SP_ get all objects for subject and predicate
        scanner.setRange(Range.prefix("spo_" + subject));
        scanner.fetchColumnFamily(new Text(predicate));
        
        return Iterables.transform(scanner, cqSelector);
      }
      else { 
        if(object != null) { // S_O get all predicates for subject and object
          
          scanner.setRange(Range.prefix("osp_" + object));
          scanner.fetchColumnFamily(new Text(predicate));
        
          return Iterables.transform(scanner, cqSelector);
        }
        else { // S__ get all predicates and objects for subject
          scanner.setRange(Range.prefix("spo_" + subject));
          return Iterables.transform(scanner, cfqSelector);
        }
      }
    }
    else {
      if(predicate != null) { 
        if(object != null) { // _PO get all subjects for predicate and object
          scanner.setRange(Range.prefix("pos_" + predicate));
          scanner.fetchColumnFamily(new Text(object));
        
          return Iterables.transform(scanner, cqSelector);
        }
        else { // _P_ get all subjects and objects for predicate
          scanner.setRange(Range.prefix("pos_" + predicate));
          return Iterables.transform(scanner, cfqSelector);
        }
      }
      else { // __O get all subjects and predicates for object
        scanner.setRange(Range.prefix("osp_" + object));
        return Iterables.transform(scanner, cfqSelector);
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    
    Connector conn = ExampleMiniCluster.getConnector();
    
    if(conn.tableOperations().exists("triples"))
      conn.tableOperations().delete("triples");
    
    conn.tableOperations().create("triples");
    
    BatchWriter writer = conn.createBatchWriter("triples", new BatchWriterConfig());
    
    System.out.println("loading data ...");
    
    FreebaseExample.insertTriples("darth_vader", writer);
    FreebaseExample.insertTriples("jedi", writer);
    FreebaseExample.insertTriples("luke_skywalker", writer);
    FreebaseExample.insertTriples("yoda", writer);
    FreebaseExample.insertTriples("obi_wan_kenobi", writer);
    
    System.out.println("done.");
    
    
    // get everything we know about Darth Vader
    System.out.println("\n=== Darth Vader ===");
    for(String s : query("darth_vader", 
            null, 
            null, 
            conn.createScanner("triples", Authorizations.EMPTY))) {
      System.out.println(s);
    }
    
    // get just a list of occupations Darth Vader has held
    System.out.println("\n=== Darth Vader's Jobs ===");
    for(String s : query("darth_vader", 
            "/fictional_universe/fictional_character/occupation", 
            null, 
            conn.createScanner("triples", Authorizations.EMPTY))) {
      System.out.println(s);
    }
    
    // find where folks were born
    System.out.println("\n=== Birthplaces ===");
    for(String s : query(null, 
            "/fictional_universe/fictional_character/place_of_birth", 
            null, 
            conn.createScanner("triples", Authorizations.EMPTY))) {
      System.out.println(s);
    }
    
    // get a list of organizations and their members
    System.out.println("\n=== Memberships ===");
    for(String s : query(null, 
            "/fictional_universe/fictional_character/organizations", 
            null, 
            conn.createScanner("triples", Authorizations.EMPTY))) {
      System.out.println(s);
    }
    
   
    // find out who Luke's parents are
    System.out.println("\n=== Luke's Parents ===");
    for(String s : query("luke_skywalker", 
            "/fictional_universe/fictional_character/parents", 
            null, 
            conn.createScanner("triples", Authorizations.EMPTY))) {
      System.out.println(s);
    }
    
    // find out anything associated with the object Yoda
    System.out.println("\n=== Yoda ===");
    for(String s : query(null, 
            null, 
            "Yoda", 
            conn.createScanner("triples", Authorizations.EMPTY))) {
      System.out.println(s);
    }
  }
}
