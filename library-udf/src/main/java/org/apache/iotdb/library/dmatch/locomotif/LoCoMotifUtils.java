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
import java.util.List;
import java.util.Locale;

public final class LoCoMotifUtils {

  private LoCoMotifUtils() {}

  public static double[][] zNormalize(double[][] values) {
    double mean = mean(values);
    double std = std(values, mean);
    double[][] normalized = new double[values.length][values[0].length];
    if (std == 0.0d || Double.isNaN(std)) {
      return normalized;
    }
    for (int i = 0; i < values.length; i++) {
      for (int j = 0; j < values[i].length; j++) {
        normalized[i][j] = (values[i][j] - mean) / std;
      }
    }
    return normalized;
  }

  public static double[] estimateGamma(double[][] values, boolean equalWeightDimensions) {
    int dimensions = values[0].length;
    double[] gamma = new double[dimensions];
    if (dimensions == 1 || !equalWeightDimensions) {
      double std = std(values, mean(values));
      double value = std == 0.0d || Double.isNaN(std) ? 1.0d : 1.0d / (std * std);
      Arrays.fill(gamma, value);
      return gamma;
    }
    for (int dimension = 0; dimension < dimensions; dimension++) {
      double mean = mean(values, dimension);
      double std = std(values, dimension, mean);
      gamma[dimension] = std == 0.0d || Double.isNaN(std) ? 1.0d : 1.0d / (std * std);
    }
    return gamma;
  }

  public static double quantile(float[] values, double rho) {
    if (values.length == 0) {
      return 0.0d;
    }
    float[] sorted = Arrays.copyOf(values, values.length);
    Arrays.sort(sorted);
    double position = rho * (sorted.length - 1);
    int lower = (int) Math.floor(position);
    int upper = (int) Math.ceil(position);
    if (lower == upper) {
      return sorted[lower];
    }
    double weight = position - lower;
    return sorted[lower] * (1.0d - weight) + sorted[upper] * weight;
  }

  public static Segment projectToVerticalAxis(int[][] path) {
    return new Segment(path[0][0], path[path.length - 1][0] + 1);
  }

  public static boolean hasMasked(boolean[] mask, int beginInclusive, int endExclusive) {
    int begin = Math.max(0, beginInclusive);
    int end = Math.min(mask.length, endExclusive);
    for (int i = begin; i < end; i++) {
      if (mask[i]) {
        return true;
      }
    }
    return false;
  }

  public static void maskMotifSet(boolean[] mask, List<Segment> motifSet, double overlap) {
    for (Segment segment : motifSet) {
      int length = segment.length();
      int maskLength = Math.max(1, (int) ((1.0d - 2.0d * overlap) * length));
      int begin = segment.getBeginIndex() + (length - maskLength) / 2;
      int end = Math.min(mask.length, begin + maskLength);
      for (int i = Math.max(0, begin); i < end; i++) {
        mask[i] = true;
      }
    }
  }

  public static String toJson(
      MotifSet motifSet, long[] times, long windowStartTime, long windowEndTime, int motifId) {
    StringBuilder builder = new StringBuilder();
    builder.append('{');
    builder.append("\"motifId\":").append(motifId).append(',');
    builder
        .append("\"fitness\":")
        .append(String.format(Locale.ROOT, "%.8f", motifSet.getFitness()))
        .append(',');
    builder.append("\"representative\":");
    appendSegment(builder, motifSet.getRepresentative(), times);
    builder.append(',');
    builder.append("\"members\":[");
    for (int i = 0; i < motifSet.getMembers().size(); i++) {
      if (i > 0) {
        builder.append(',');
      }
      appendSegment(builder, motifSet.getMembers().get(i), times);
    }
    builder.append("],");
    builder
        .append("\"window\":{\"beginTime\":")
        .append(windowStartTime)
        .append(",\"endTime\":")
        .append(windowEndTime)
        .append("}}");
    return builder.toString();
  }

  private static void appendSegment(StringBuilder builder, Segment segment, long[] times) {
    int beginIndex = segment.getBeginIndex();
    int endIndex = Math.max(segment.getBeginIndex(), segment.getEndIndex() - 1);
    builder.append('{');
    builder.append("\"beginIndex\":").append(segment.getBeginIndex()).append(',');
    builder.append("\"endIndex\":").append(segment.getEndIndex()).append(',');
    builder.append("\"beginTime\":").append(times[beginIndex]).append(',');
    builder.append("\"endTime\":").append(times[endIndex]);
    builder.append('}');
  }

  private static double mean(double[][] values) {
    double sum = 0.0d;
    int count = 0;
    for (double[] row : values) {
      for (double value : row) {
        sum += value;
        count++;
      }
    }
    return sum / count;
  }

  private static double mean(double[][] values, int dimension) {
    double sum = 0.0d;
    for (double[] row : values) {
      sum += row[dimension];
    }
    return sum / values.length;
  }

  private static double std(double[][] values, double mean) {
    double sum = 0.0d;
    int count = 0;
    for (double[] row : values) {
      for (double value : row) {
        double diff = value - mean;
        sum += diff * diff;
        count++;
      }
    }
    return Math.sqrt(sum / count);
  }

  private static double std(double[][] values, int dimension, double mean) {
    double sum = 0.0d;
    for (double[] row : values) {
      double diff = row[dimension] - mean;
      sum += diff * diff;
    }
    return Math.sqrt(sum / values.length);
  }
}
