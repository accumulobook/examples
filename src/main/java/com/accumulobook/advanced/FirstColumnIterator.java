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
import java.util.Collection;
import java.util.Map;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;


public class FirstColumnIterator extends WrappingIterator  {
  
  private Range range;
  private boolean inclusive;
  private Collection<ByteSequence> columnFamilies;
  private boolean done;

  public FirstColumnIterator() {}
  
  public FirstColumnIterator(FirstColumnIterator aThis, IteratorEnvironment env) {
    super();
    setSource(aThis.getSource().deepCopy(env));
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
  }
  
  @Override
  public void next() throws IOException {
    if(done) {
      return;
    }
    
    // setup a new range to seek to 
    Key nextKey = getSource().getTopKey().followingKey(PartialKey.ROW);
    if(range.afterEndKey(nextKey)) {
      done = true;
    }
    else {
      Range nextRange = new Range(nextKey, true, range.getEndKey(), range.isEndKeyInclusive());
      getSource().seek(nextRange, columnFamilies, inclusive);
    }
  }

  @Override
  public boolean hasTop() {
    return !done && getSource().hasTop();
  }
  
  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    this.range = range;
    this.columnFamilies = columnFamilies;
    this.inclusive = inclusive;

    done = false;

    Key startKey = range.getStartKey();
    Range seekRange = new Range(startKey == null ? null : new Key(startKey.getRow()), true, range.getEndKey(), range.isEndKeyInclusive());
    super.seek(seekRange, columnFamilies, inclusive);

    if (getSource().hasTop()) {
      if (range.beforeStartKey(getSource().getTopKey()))
        next();
    }
  }
  
  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new FirstColumnIterator(this, env);
  }
}
