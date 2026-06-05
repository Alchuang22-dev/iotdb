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

import java.util.Arrays;

/** Local warping path between two time-series subsequences. */
public class LocalPath {

  private final int[][] points;
  private final float[] similarities;
  private final float[] cumulativeSimilarities;
  private final int[] indexI;
  private final int[] indexJ;
  private final int i1;
  private final int il;
  private final int j1;
  private final int jl;

  public LocalPath(int[][] points, float[] similarities) {
    if (points.length == 0 || points.length != similarities.length) {
      throw new IllegalArgumentException(
          "Path and similarity arrays must be non-empty and aligned.");
    }
    this.points = copyPoints(points);
    this.similarities = Arrays.copyOf(similarities, similarities.length);
    this.cumulativeSimilarities = new float[similarities.length + 1];
    for (int i = 0; i < similarities.length; i++) {
      this.cumulativeSimilarities[i + 1] = this.cumulativeSimilarities[i] + similarities[i];
    }
    this.i1 = points[0][0];
    this.il = points[points.length - 1][0] + 1;
    this.j1 = points[0][1];
    this.jl = points[points.length - 1][1] + 1;
    this.indexI = new int[il - i1];
    this.indexJ = new int[jl - j1];
    constructIndex();
  }

  public int length() {
    return points.length;
  }

  public int getI1() {
    return i1;
  }

  public int getIl() {
    return il;
  }

  public int getJ1() {
    return j1;
  }

  public int getJl() {
    return jl;
  }

  public int getRowAt(int index) {
    return points[index][0];
  }

  public int getColumnAt(int index) {
    return points[index][1];
  }

  public int findI(int i) {
    if (i < i1 || i >= il) {
      throw new IllegalArgumentException(String.format("Row index %d is outside path.", i));
    }
    return indexI[i - i1];
  }

  public int findJ(int j) {
    if (j < j1 || j >= jl) {
      throw new IllegalArgumentException(String.format("Column index %d is outside path.", j));
    }
    return indexJ[j - j1];
  }

  public float similaritySum(int beginPathIndex, int endPathIndex) {
    return cumulativeSimilarities[endPathIndex + 1] - cumulativeSimilarities[beginPathIndex];
  }

  public int[][] subPathBetweenColumnIndices(int beginColumn, int endColumn) {
    int beginPathIndex = findJ(beginColumn);
    int endPathIndex = findJ(endColumn);
    return copyPoints(Arrays.copyOfRange(points, beginPathIndex, endPathIndex + 1));
  }

  private void constructIndex() {
    int currentI = points[0][0];
    int currentJ = points[0][1];
    for (int pathIndex = 1; pathIndex < points.length; pathIndex++) {
      if (points[pathIndex][0] != currentI) {
        for (int i = currentI - i1 + 1; i <= points[pathIndex][0] - i1; i++) {
          indexI[i] = pathIndex;
        }
        currentI = points[pathIndex][0];
      }
      if (points[pathIndex][1] != currentJ) {
        for (int j = currentJ - j1 + 1; j <= points[pathIndex][1] - j1; j++) {
          indexJ[j] = pathIndex;
        }
        currentJ = points[pathIndex][1];
      }
    }
  }

  private static int[][] copyPoints(int[][] source) {
    int[][] target = new int[source.length][2];
    for (int i = 0; i < source.length; i++) {
      target[i][0] = source[i][0];
      target[i][1] = source[i][1];
    }
    return target;
  }
}
