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
package com.accumulobook.designs.lexicoder;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.iterators.ValueFormatException;

public class Inet4AddressLexicoder implements Lexicoder<Inet4Address> {

  @Override
  public byte[] encode(Inet4Address v) {
    return v.getAddress();
  }

  @Override
  public Inet4Address decode(byte[] b) throws ValueFormatException {
    try {
      return (Inet4Address) Inet4Address.getByAddress(b);
    } catch (UnknownHostException ex) {
      throw new ValueFormatException(ex.getMessage());
    }
  }
  
}
