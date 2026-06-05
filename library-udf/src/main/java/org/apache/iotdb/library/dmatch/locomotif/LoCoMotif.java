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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Public entry point for LoCoMotif motif-set discovery. */
public class LoCoMotif {

  private final double[][] timeSeries;
  private final int lMin;
  private final int lMax;
  private final double rho;
  private final int maxMotifSets;
  private final double overlap;
  private final boolean warping;
  private final boolean equalWeightDimensions;

  public LoCoMotif(
      double[][] timeSeries,
      int lMin,
      int lMax,
      double rho,
      int maxMotifSets,
      double overlap,
      boolean warping,
      boolean equalWeightDimensions) {
    this.timeSeries = timeSeries;
    this.lMin = Math.max(4, lMin);
    this.lMax = lMax;
    this.rho = rho;
    this.maxMotifSets = maxMotifSets;
    this.overlap = overlap;
    this.warping = warping;
    this.equalWeightDimensions = equalWeightDimensions;
  }

  public List<MotifSet> findMotifSets() {
    int n = timeSeries.length;
    if (n < lMin || maxMotifSets <= 0 || lMax < lMin) {
      return new ArrayList<>();
    }
    LoCo loCo = LoCo.instanceFromRho(timeSeries, rho, warping, equalWeightDimensions);
    List<LocalPath> paths =
        buildLocalPaths(
            loCo.findBestPaths(lMin, Math.max(10, lMin / 2)), loCo.getSimilarityMatrix());

    List<MotifSet> motifSets = new ArrayList<>();
    boolean[] mask = new boolean[n];
    boolean[] startMask = new boolean[n];
    boolean[] endMask = new boolean[n];
    Arrays.fill(startMask, true);
    Arrays.fill(endMask, true);

    while (motifSets.size() < maxMotifSets) {
      for (int i = 0; i < n; i++) {
        if (mask[i]) {
          startMask[i] = false;
          endMask[i] = false;
        }
      }
      MotifSet best =
          CandidateFinder.findBest(
              paths, n, lMin, Math.min(lMax, n), overlap, mask, mask, startMask, endMask);
      if (best == null || best.getFitness() == 0.0d) {
        break;
      }
      motifSets.add(best);
      LoCoMotifUtils.maskMotifSet(mask, best.getMembers(), overlap);
    }
    return motifSets;
  }

  private static List<LocalPath> buildLocalPaths(
      List<int[][]> rawPaths, float[][] similarityMatrix) {
    List<LocalPath> paths = new ArrayList<>();
    for (int[][] rawPath : rawPaths) {
      float[] similarities = new float[rawPath.length];
      for (int i = 0; i < rawPath.length; i++) {
        similarities[i] = similarityMatrix[rawPath[i][0]][rawPath[i][1]];
      }
      paths.add(new LocalPath(rawPath, similarities));
      if (!isDiagonal(rawPath)) {
        paths.add(new LocalPath(mirror(rawPath), similarities));
      }
    }
    return paths;
  }

  private static boolean isDiagonal(int[][] path) {
    for (int[] point : path) {
      if (point[0] != point[1]) {
        return false;
      }
    }
    return true;
  }

  private static int[][] mirror(int[][] path) {
    int[][] mirrored = new int[path.length][2];
    for (int i = 0; i < path.length; i++) {
      mirrored[i][0] = path[i][1];
      mirrored[i][1] = path[i][0];
    }
    return mirrored;
  }
}
