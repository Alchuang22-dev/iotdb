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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** LoCo local warping path discovery over a self-similarity matrix. */
public class LoCo {

  private static final int MATRIX_TILE_SIZE = 128;

  private final double[][] timeSeries;
  private final double[] gamma;
  private final boolean warping;
  private final int minLag;
  private final int maxLag;
  private final double similarityThreshold;
  private final double similarityThresholdDistance;
  private double tau;
  private double deltaA;
  private double deltaM;
  private float[][] similarityMatrix;
  private float[][] cumulativeSimilarityMatrix;
  private float[] similarityValues;
  private int similarityValueCount;

  private LoCo(
      double[][] timeSeries,
      boolean warping,
      boolean equalWeightDimensions,
      int minLag,
      int maxLag,
      double similarityThreshold) {
    this.timeSeries = timeSeries;
    this.warping = warping;
    this.gamma = LoCoMotifUtils.estimateGamma(timeSeries, equalWeightDimensions);
    this.minLag = Math.max(0, minLag);
    this.maxLag = maxLag < 0 ? Integer.MAX_VALUE : maxLag;
    this.similarityThreshold = Math.max(0.0d, Math.min(1.0d, similarityThreshold));
    this.similarityThresholdDistance =
        this.similarityThreshold > 0.0d
            ? -Math.log(this.similarityThreshold)
            : Double.POSITIVE_INFINITY;
    this.deltaM = 0.5d;
  }

  public static LoCo instanceFromRho(
      double[][] timeSeries, double rho, boolean warping, boolean equalWeightDimensions) {
    return instanceFromRho(timeSeries, rho, warping, equalWeightDimensions, 0, -1, 0.0d);
  }

  public static LoCo instanceFromRho(
      double[][] timeSeries,
      double rho,
      boolean warping,
      boolean equalWeightDimensions,
      int minLag,
      int maxLag,
      double similarityThreshold) {
    LoCo loco =
        new LoCo(timeSeries, warping, equalWeightDimensions, minLag, maxLag, similarityThreshold);
    loco.calculateSimilarityMatrix();
    loco.tau = loco.estimateTau(rho);
    loco.similarityValues = null;
    loco.deltaA = 2.0d * loco.tau;
    return loco;
  }

  public float[][] getSimilarityMatrix() {
    if (similarityMatrix == null) {
      calculateSimilarityMatrix();
    }
    return similarityMatrix;
  }

  public List<int[][]> findBestPaths(int lMin, int vWidth) {
    if (cumulativeSimilarityMatrix == null) {
      calculateCumulativeSimilarityMatrix();
    }
    int size = cumulativeSimilarityMatrix.length;
    boolean[][] mask = new boolean[size][size];

    List<int[][]> paths = findBestPaths(cumulativeSimilarityMatrix, mask, lMin, vWidth, warping);
    List<int[][]> shiftedPaths = new ArrayList<>();
    shiftedPaths.add(diagonalPath(timeSeries.length));
    for (int[][] path : paths) {
      shiftedPaths.add(shiftPath(path, -2));
    }
    return shiftedPaths;
  }

  private float[][] calculateSimilarityMatrix() {
    int n = timeSeries.length;
    int dimensions = timeSeries[0].length;
    similarityMatrix = new float[n][n];
    similarityValues = new float[Math.max(16, Math.min(n * 2, 4096))];
    similarityValueCount = 0;

    for (int i = 0; i < n; i++) {
      float[] similarityRow = similarityMatrix[i];
      similarityRow[i] = 1.0f;
      collectSimilarityValue(1.0f);

      int rowBegin = offDiagonalColumnBegin(i, n);
      int rowEnd = offDiagonalColumnEnd(i, n);
      for (int colBlock = rowBegin; colBlock < rowEnd; colBlock += MATRIX_TILE_SIZE) {
        int colEnd = Math.min(rowEnd, colBlock + MATRIX_TILE_SIZE);
        if (dimensions == 1) {
          calculateSingleDimensionSimilarityRow(i, colBlock, colEnd, similarityRow);
        } else {
          calculateMultiDimensionSimilarityRow(i, colBlock, colEnd, similarityRow, dimensions);
        }
      }
    }
    return similarityMatrix;
  }

  private void calculateSingleDimensionSimilarityRow(
      int i, int colBegin, int colEnd, float[] similarityRow) {
    double value = timeSeries[i][0];
    double weight = gamma[0];
    for (int j = colBegin; j < colEnd; j++) {
      double diff = value - timeSeries[j][0];
      double distance = weight * diff * diff;
      collectSimilarity(j, distance, similarityRow);
    }
  }

  private void calculateMultiDimensionSimilarityRow(
      int i, int colBegin, int colEnd, float[] similarityRow, int dimensions) {
    double[] left = timeSeries[i];
    for (int j = colBegin; j < colEnd; j++) {
      double[] right = timeSeries[j];
      double distance = 0.0d;
      for (int dimension = 0; dimension < dimensions; dimension++) {
        double diff = left[dimension] - right[dimension];
        distance += gamma[dimension] * diff * diff;
        if (distance > similarityThresholdDistance) {
          break;
        }
      }
      collectSimilarity(j, distance, similarityRow);
    }
  }

  private void collectSimilarity(int j, double distance, float[] similarityRow) {
    if (distance > similarityThresholdDistance) {
      similarityRow[j] = Float.NEGATIVE_INFINITY;
      return;
    }
    float similarity = (float) Math.exp(-distance);
    if (similarity < similarityThreshold) {
      similarityRow[j] = Float.NEGATIVE_INFINITY;
      return;
    }
    similarityRow[j] = similarity;
    collectSimilarityValue(similarity);
  }

  private void collectSimilarityValue(float similarity) {
    if (Float.isFinite(similarity)) {
      if (similarityValueCount == similarityValues.length) {
        float[] expanded = new float[similarityValues.length * 2];
        System.arraycopy(similarityValues, 0, expanded, 0, similarityValues.length);
        similarityValues = expanded;
      }
      similarityValues[similarityValueCount++] = similarity;
    }
  }

  private void calculateCumulativeSimilarityMatrix() {
    if (similarityMatrix == null) {
      calculateSimilarityMatrix();
    }
    int n = similarityMatrix.length;
    cumulativeSimilarityMatrix = new float[n + 2][n + 2];
    for (int i = 0; i < n; i++) {
      calculateCumulativeSimilarityCell(i, i);
      int rowBegin = offDiagonalColumnBegin(i, n);
      int rowEnd = offDiagonalColumnEnd(i, n);
      for (int colBlock = rowBegin; colBlock < rowEnd; colBlock += MATRIX_TILE_SIZE) {
        int colEnd = Math.min(rowEnd, colBlock + MATRIX_TILE_SIZE);
        for (int j = colBlock; j < colEnd; j++) {
          calculateCumulativeSimilarityCell(i, j);
        }
      }
    }
  }

  private void calculateCumulativeSimilarityCell(int i, int j) {
    float similarity = similarityMatrix[i][j];
    if (!Float.isFinite(similarity)) {
      cumulativeSimilarityMatrix[i + 2][j + 2] = 0.0f;
      return;
    }
    float[] previousRow = cumulativeSimilarityMatrix[i + 1];
    float[] previousPreviousRow = cumulativeSimilarityMatrix[i];
    double previous;
    if (warping) {
      previous = max3(previousRow[j + 1], previousPreviousRow[j + 1], previousRow[j]);
    } else {
      previous = previousRow[j + 1];
    }

    double value;
    if (similarity < tau) {
      value = Math.max(0.0d, deltaM * previous - deltaA);
    } else {
      value = Math.max(0.0d, similarity + previous);
    }
    cumulativeSimilarityMatrix[i + 2][j + 2] = (float) value;
  }

  private double estimateTau(double rho) {
    if (similarityValueCount == 0) {
      return 0.0d;
    }
    return LoCoMotifUtils.quantileInPlace(similarityValues, similarityValueCount, rho);
  }

  private List<int[][]> findBestPaths(
      float[][] matrix, boolean[][] mask, int lMin, int vWidth, boolean warping) {
    List<PathSeed> seeds = new ArrayList<>();
    for (int i = 2; i < matrix.length; i++) {
      int columnBegin = pathColumnBegin(i, matrix.length, vWidth);
      int columnEnd = pathColumnEnd(i, matrix.length);
      for (int j = columnBegin; j < columnEnd; j++) {
        if (matrix[i][j] > 0.0f && !mask[i][j]) {
          seeds.add(new PathSeed(i, j, matrix[i][j]));
        }
      }
    }
    Collections.sort(
        seeds,
        new Comparator<PathSeed>() {
          @Override
          public int compare(PathSeed o1, PathSeed o2) {
            return Float.compare(o1.value, o2.value);
          }
        });

    List<int[][]> paths = new ArrayList<>();
    for (int seedIndex = seeds.size() - 1; seedIndex >= 0; seedIndex--) {
      PathSeed seed = seeds.get(seedIndex);
      if (mask[seed.i][seed.j]) {
        continue;
      }
      if (seed.i < 2 || seed.j < 2) {
        break;
      }
      int[][] path =
          warping
              ? bestPathWarping(matrix, mask, seed.i, seed.j, vWidth)
              : bestPathNoWarping(matrix, mask, seed.i, seed.j, vWidth);
      maskVicinity(path, mask, 0);
      if (path[path.length - 1][0] - path[0][0] + 1 >= lMin
          || path[path.length - 1][1] - path[0][1] + 1 >= lMin) {
        maskVicinity(path, mask, vWidth);
        paths.add(path);
      }
    }
    return paths;
  }

  private int[][] bestPathWarping(float[][] matrix, boolean[][] mask, int i, int j, int vWidth) {
    List<int[]> reversePath = new ArrayList<>();
    while (i >= 2 && j >= 2) {
      reversePath.add(new int[] {i, j});
      float diagonal = matrix[i - 1][j - 1];
      float vertical = matrix[i - 2][j - 1];
      float horizontal = matrix[i - 1][j - 2];
      float maximum = max3(diagonal, vertical, horizontal);
      if (diagonal == maximum) {
        if (isUnavailablePathCell(matrix, mask, i - 1, j - 1, vWidth)) {
          break;
        }
        i--;
        j--;
      } else if (vertical == maximum) {
        if (isUnavailablePathCell(matrix, mask, i - 2, j - 1, vWidth)) {
          break;
        }
        i -= 2;
        j--;
      } else {
        if (isUnavailablePathCell(matrix, mask, i - 1, j - 2, vWidth)) {
          break;
        }
        i--;
        j -= 2;
      }
    }
    return reverse(reversePath);
  }

  private int[][] bestPathNoWarping(float[][] matrix, boolean[][] mask, int i, int j, int vWidth) {
    List<int[]> reversePath = new ArrayList<>();
    while (i >= 2 && j >= 2) {
      reversePath.add(new int[] {i, j});
      if (isUnavailablePathCell(matrix, mask, i - 1, j - 1, vWidth)) {
        break;
      }
      i--;
      j--;
    }
    return reverse(reversePath);
  }

  private int offDiagonalColumnBegin(int row, int size) {
    int lag = Math.max(1, minLag);
    if (row > Integer.MAX_VALUE - lag) {
      return size;
    }
    return Math.min(size, row + lag);
  }

  private int offDiagonalColumnEnd(int row, int size) {
    if (maxLag == Integer.MAX_VALUE) {
      return size;
    }
    if (row > Integer.MAX_VALUE - maxLag - 1) {
      return size;
    }
    return Math.min(size, row + maxLag + 1);
  }

  private int pathColumnBegin(int row, int size, int vWidth) {
    int lag = Math.max(Math.max(1, minLag), vWidth + 1);
    if (row > Integer.MAX_VALUE - lag) {
      return size;
    }
    return Math.min(size, row + lag);
  }

  private int pathColumnEnd(int row, int size) {
    if (maxLag == Integer.MAX_VALUE) {
      return size;
    }
    if (row > Integer.MAX_VALUE - maxLag - 1) {
      return size;
    }
    return Math.min(size, row + maxLag + 1);
  }

  private boolean isUnavailablePathCell(
      float[][] matrix, boolean[][] mask, int row, int column, int vWidth) {
    if (row < 2 || row >= matrix.length || column < 2 || column >= matrix[row].length) {
      return true;
    }
    int lag = column - row;
    if (lag < Math.max(Math.max(1, minLag), vWidth + 1)) {
      return true;
    }
    if (maxLag != Integer.MAX_VALUE && lag > maxLag) {
      return true;
    }
    return mask[row][column] || matrix[row][column] <= 0.0f;
  }

  private static void maskVicinity(int[][] path, boolean[][] mask, int vWidth) {
    for (int k = 0; k < path.length - 1; k++) {
      int currentI = path[k][0];
      int currentJ = path[k][1];
      int nextI = path[k + 1][0];
      int nextJ = path[k + 1][1];
      int deltaI = nextI - currentI;
      int deltaJ = nextJ - currentJ;

      markColumn(mask, currentJ, currentI - vWidth, currentI + vWidth);
      markRow(mask, currentI, currentJ - vWidth, currentJ + vWidth);

      if (deltaI == 2 && deltaJ == 1) {
        mark(mask, currentI + 1, currentJ);
        markRow(mask, currentI + 1, currentJ - vWidth, currentJ + vWidth);
      } else if (deltaI == 1 && deltaJ == 2) {
        mark(mask, currentI, currentJ + 1);
        markColumn(mask, currentJ + 1, currentI - vWidth, currentI + vWidth);
      } else if (!(deltaI == 1 && deltaJ == 1)) {
        throw new IllegalStateException("Path does not comply with the allowed step sizes.");
      }
    }
    int[] last = path[path.length - 1];
    markColumn(mask, last[1], last[0] - vWidth, last[0] + vWidth);
    markRow(mask, last[0], last[1] - vWidth, last[1] + vWidth);
  }

  private static int[][] reverse(List<int[]> reversePath) {
    int[][] path = new int[reversePath.size()][2];
    for (int i = 0; i < reversePath.size(); i++) {
      int[] point = reversePath.get(reversePath.size() - 1 - i);
      path[i][0] = point[0];
      path[i][1] = point[1];
    }
    return path;
  }

  private static int[][] shiftPath(int[][] path, int offset) {
    int[][] shifted = new int[path.length][2];
    for (int i = 0; i < path.length; i++) {
      shifted[i][0] = path[i][0] + offset;
      shifted[i][1] = path[i][1] + offset;
    }
    return shifted;
  }

  private static int[][] diagonalPath(int size) {
    int[][] path = new int[size][2];
    for (int i = 0; i < size; i++) {
      path[i][0] = i;
      path[i][1] = i;
    }
    return path;
  }

  private static float max3(float a, float b, float c) {
    if (a >= b) {
      return a >= c ? a : c;
    }
    return b >= c ? b : c;
  }

  private static void markColumn(boolean[][] mask, int column, int beginRow, int endRow) {
    if (column < 0 || column >= mask[0].length) {
      return;
    }
    int begin = Math.max(0, beginRow);
    int end = Math.min(mask.length - 1, endRow);
    for (int row = begin; row <= end; row++) {
      mask[row][column] = true;
    }
  }

  private static void markRow(boolean[][] mask, int row, int beginColumn, int endColumn) {
    if (row < 0 || row >= mask.length) {
      return;
    }
    int begin = Math.max(0, beginColumn);
    int end = Math.min(mask[row].length - 1, endColumn);
    for (int column = begin; column <= end; column++) {
      mask[row][column] = true;
    }
  }

  private static void mark(boolean[][] mask, int row, int column) {
    if (row >= 0 && row < mask.length && column >= 0 && column < mask[row].length) {
      mask[row][column] = true;
    }
  }

  private static class PathSeed {
    private final int i;
    private final int j;
    private final float value;

    private PathSeed(int i, int j, float value) {
      this.i = i;
      this.j = j;
      this.value = value;
    }
  }
}
