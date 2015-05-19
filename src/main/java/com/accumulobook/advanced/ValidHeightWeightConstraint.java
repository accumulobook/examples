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

import java.util.ArrayList;
import java.util.List;
import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

/**
 * Ensures that a mutation contains both a height and weight
 * and that the values are valid.
 * 
 * We require that any mutation that updates height also updates weight
 * and vice versa, and that the values for height and weight are 
 * string representations of non-negative numbers.
 */
public class ValidHeightWeightConstraint implements Constraint {

  private static final short INVALID_HEIGHT_VALUE = 1;
  private static final short INVALID_WEIGHT_VALUE = 2;
  private static final short MISSING_HEIGHT = 3;
  private static final short MISSING_WEIGHT = 4;
  
  @Override
  public String getViolationDescription(short violationCode) {
    switch(violationCode) {
      case INVALID_HEIGHT_VALUE:
        return "Invalid height value";
      case INVALID_WEIGHT_VALUE:
        return "Invalid weight value";
      case MISSING_HEIGHT:
        return "Missing height column";
      case MISSING_WEIGHT:
        return "Missing weight column";
    }
    
    return null;
  }

  final static List<Short> NO_VIOLATIONS = new ArrayList<>();
  final static byte[] heightBytes = "height".getBytes();
  final static byte[] weightBytes = "weight".getBytes();
  
  @Override
  public List<Short> check(Environment env, Mutation mutation) {
    
    List<Short> violations = null;
    
    List<ColumnUpdate> updates = mutation.getUpdates();
    
    boolean haveHeight = false;
    boolean haveWeight = false;
    
    for(ColumnUpdate update : updates) {
      
      // check height update
      if(equalBytes(update.getColumnQualifier(), heightBytes)) {
        haveHeight = true;
        if(!isNonNegativeNumberString(update.getValue())) {
          if(violations == null)
             violations = new ArrayList<>();
          violations.add(INVALID_HEIGHT_VALUE);
        }
      }
      
      // check weight update
      if(equalBytes(update.getColumnQualifier(), weightBytes)) {
        haveWeight = true;
        if(!isNonNegativeNumberString(update.getValue())) {
          if(violations == null)
             violations = new ArrayList<>();
          violations.add(INVALID_WEIGHT_VALUE);
        }
      }
    }
    
    // if we have height, we must also have weight
    if(haveHeight && ! haveWeight) {
      if(violations == null)
        violations = new ArrayList<>();
      violations.add(MISSING_WEIGHT);
    }
    
    // if we have weight, we must also have height
    if(haveWeight && !haveHeight) {
      if(violations == null)
        violations = new ArrayList<>();
      violations.add(MISSING_HEIGHT);
    }
    
    return violations == null ? NO_VIOLATIONS : violations;
  }

  private boolean equalBytes(byte[] a, byte[] b) {
    return Value.Comparator.compareBytes(a, 0, a.length, b, 0, b.length) == 0;
  }
  // return whether the value is a string representation of a non-negative number
  private boolean isNonNegativeNumberString(byte[] value) {
    try {
      double val = Double.parseDouble(new String(value));
      return val >= 0.0;
    }
    catch(NumberFormatException nfe) {
      return false;
    }
  }
  
  public static void test() {
    
    ValidHeightWeightConstraint c = new ValidHeightWeightConstraint();
    
    Mutation m = new Mutation("sdf");
    System.out.println(c.check(null, m));
    
    m = new Mutation("sdf");
    m.put("", "height", "-1.0");
    System.out.println(c.check(null, m));
    
    m = new Mutation("sdf");
    m.put("", "height", "6.0");
    System.out.println(c.check(null, m));
    
    m = new Mutation("sdf");
    m.put("", "height", "6.0");
    m.put("", "weight", "-6.0");
    System.out.println(c.check(null, m));
    
    m = new Mutation("sdf");
    m.put("", "height", "6.0");
    m.put("", "weight", "6.0");
    System.out.println(c.check(null, m));
    
    m = new Mutation("sdf");
    m.put("", "weight", "6.0");
    System.out.println(c.check(null, m));
    
    m = new Mutation("sdf");
    m.put("", "weight", "-6.0");
    System.out.println(c.check(null, m));
  }
}
