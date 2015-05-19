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
package com.accumulobook.advanced;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.nio.charset.StandardCharsets;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.accumulo.core.util.Pair;

/**
 * 
 * Keep track of the number of values and the total sum of all values
 * Clients can divide sum by the count to get the current average
 * 
 */
public class RunningAverageCombiner extends TypedValueCombiner<Pair<Long,Double>> {
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    setEncoder(new LongDoublePairEncoder());
  }
  
  @Override
  public Pair<Long,Double> typedReduce(Key key, Iterator<Pair<Long,Double>> iter) {
  
    Long count = 0L;
    Double sum = 0.0;
    
    while(iter.hasNext()) {
      Pair<Long,Double> pair = iter.next();
      
      count += pair.getFirst();
      sum += pair.getSecond();
    }
    
    return new Pair<>(count, sum);
  }
  
  public static class LongDoublePairEncoder implements Encoder<Pair<Long,Double>> {

    @Override
    public byte[] encode(Pair<Long, Double> v) {
       String s = Long.toString(v.getFirst()) + ":" + Double.toString(v.getSecond());

      return s.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Pair<Long, Double> decode(byte[] b) throws ValueFormatException {
      String s = new String(b, StandardCharsets.UTF_8);
      String[] parts = s.split(":");
      return new Pair<>(Long.parseLong(parts[0]), Double.parseDouble(parts[1]));
    }
  }
}
