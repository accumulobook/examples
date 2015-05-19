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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.io.Text;

public class Graph {

  /**
   * create a graph table
   *
   * @param conn
   * @param name
   * @param deleteIfPresent
   * @return
   * @throws Exception
   */
  public static BatchWriter setupTable(
          final Connector conn,
          final String name,
          boolean deleteIfPresent) throws Exception {

    TableOperations ops = conn.tableOperations();

    // create table
    if (ops.exists(name) && deleteIfPresent) {
      ops.delete(name);
    }

    ops.create(name);

    // remove versioning iterator
    ops.removeIterator(name, "vers", EnumSet.allOf(IteratorUtil.IteratorScope.class));

    // setup summing combiner
    IteratorSetting iterSet = new IteratorSetting(10, "sum", SummingCombiner.class);
    SummingCombiner.setCombineAllColumns(iterSet, true);
    SummingCombiner.setEncodingType(iterSet, SummingCombiner.STRING_ENCODER.getClass().getName());

    ops.attachIterator(name, iterSet);

    return conn.createBatchWriter(name, new BatchWriterConfig());
  }

  /**
   * store an edge in the graph
   *
   * @param nodeA
   * @param nodeB
   * @param edgeType
   * @param weight
   * @param writer
   * @param storeReverseEdge
   */
  public static void writeEdge(
          final String nodeA,
          final String nodeB,
          final String edgeType,
          int weight,
          final BatchWriter writer,
          boolean storeReverseEdge) {
    try {
      Mutation forward = new Mutation(nodeA);
      forward.put(edgeType, nodeB, new Value(Integer.toString(weight).getBytes()));
      writer.addMutation(forward);

      if (storeReverseEdge) {
        Mutation reverse = new Mutation(nodeB);
        reverse.put(edgeType, nodeA, new Value(Integer.toString(weight).getBytes()));
        writer.addMutation(reverse);
      }
    } catch (MutationsRejectedException ex) {
      Logger.getLogger(TwitterGraph.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  /**
   * return the first node starting with the given string
   *
   * @param start
   * @param scanner
   * @return
   */
  public static Optional<String> discoverNode(
          final String start,
          final Scanner scanner) {

    scanner.setRange(Range.prefix(start));

    Iterator<Map.Entry<Key, Value>> iter = scanner.iterator();
    if (!iter.hasNext()) {
      return Optional.absent();
    }

    return Optional.of(iter.next().getKey().getColumnQualifier().toString());
  }

  /**
   * return a list of neighbors for a given node
   *
   * @param node
   * @param scanner
   * @param edgeType
   * @return
   */
  public static Iterable<String> getNeighbors(
          final String node,
          final Scanner scanner,
          final String edgeType) {

    scanner.setRange(Range.exact(node));

    if (!edgeType.equals("ALL")) {
      scanner.fetchColumnFamily(new Text(edgeType));
    }

    return Iterables.transform(scanner, new Function<Entry<Key, Value>, String>() {
      @Override
      public String apply(Entry<Key, Value> f) {
        return f.getKey().getColumnQualifier().toString();
      }
    });
  }

  /**
   * 
   * @param neighbors
   * @param batchScanner
   * @param edgeType
   * @return 
   */
  public static Iterable<String> neighborsOfNeighbors(
          final Iterable<String> neighbors,
          final BatchScanner batchScanner,
          final String edgeType) {

    List<Iterable<String>> nextNeighbors = new ArrayList<>();

    // process given neighbors in batches of 100
    for (List<String> batch : Iterables.partition(neighbors, 100)) {
      batchScanner.setRanges(Lists.transform(batch, new Function<String, Range>() {
        @Override
        public Range apply(String f) {
          return Range.exact(f);
        }
      }));

      if (!edgeType.equals("ALL")) 
        batchScanner.fetchColumnFamily(new Text(edgeType));

      nextNeighbors.add(Iterables.transform(batchScanner, new Function<Entry<Key, Value>, String>() {
        @Override
        public String apply(Entry<Key, Value> f) {
          return f.getKey().getColumnQualifier().toString();
        }
      }));
    }
    
    return Sets.newHashSet(Iterables.concat(nextNeighbors));
  }
}
