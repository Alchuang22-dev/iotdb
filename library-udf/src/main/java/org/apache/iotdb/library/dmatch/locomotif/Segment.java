/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.library.dmatch.locomotif;

/** Half-open time-series segment in sample-index space. */
public class Segment {

  private final int beginIndex;
  private final int endIndex;

  public Segment(int beginIndex, int endIndex) {
    if (beginIndex < 0 || endIndex < beginIndex) {
      throw new IllegalArgumentException(
          String.format("Illegal segment range [%d, %d).", beginIndex, endIndex));
    }
    this.beginIndex = beginIndex;
    this.endIndex = endIndex;
  }

  public int getBeginIndex() {
    return beginIndex;
  }

  public int getEndIndex() {
    return endIndex;
  }

  public int length() {
    return endIndex - beginIndex;
  }
}
