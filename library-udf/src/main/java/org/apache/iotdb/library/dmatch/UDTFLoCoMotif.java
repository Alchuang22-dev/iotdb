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

package org.apache.iotdb.library.dmatch;

import org.apache.iotdb.library.dmatch.locomotif.LoCoMotif;
import org.apache.iotdb.library.dmatch.locomotif.LoCoMotifUtils;
import org.apache.iotdb.library.dmatch.locomotif.MotifSet;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.type.Type;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** This function discovers variable-length motif sets using LoCoMotif. */
public class UDTFLoCoMotif implements UDTF {

  private static final String L_MIN = "l_min";
  private static final String L_MAX = "l_max";
  private static final int DEFAULT_WINDOW = 5000;
  private static final int DEFAULT_NB = 10;
  private static final int DEFAULT_MAX_POINTS = 10000;

  private int lMin;
  private int lMax;
  private double rho;
  private int nb;
  private double overlap;
  private boolean warping;
  private boolean normalize;
  private boolean equalWeightDimensions;
  private int window;
  private int step;
  private int maxPoints;
  private int dimension;
  private Set<Long> emittedTimes;

  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    UDFParameters parameters = validator.getParameters();
    validator.validateInputSeriesNumber(1, Integer.MAX_VALUE);
    for (int i = 0; i < parameters.getChildExpressionsSize(); i++) {
      validator.validateInputSeriesDataType(i, Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE);
    }
    validator
        .validateRequiredAttribute(L_MIN)
        .validateRequiredAttribute(L_MAX)
        .validate(
            value -> (int) value >= 4,
            "l_min has to be an integer greater than or equal to 4.",
            parameters.getIntOrDefault(L_MIN, -1))
        .validate(
            args -> (int) args[1] >= (int) args[0],
            "l_max has to be greater than or equal to l_min.",
            parameters.getIntOrDefault(L_MIN, -1),
            parameters.getIntOrDefault(L_MAX, -1))
        .validate(
            value -> (double) value > 0.0d && (double) value < 1.0d,
            "rho has to be in the range (0, 1).",
            parameters.getDoubleOrDefault("rho", 0.8d))
        .validate(
            value -> (int) value > 0,
            "nb has to be a positive integer.",
            parameters.getIntOrDefault("nb", DEFAULT_NB))
        .validate(
            value -> (double) value >= 0.0d && (double) value <= 0.5d,
            "overlap has to be in the range [0, 0.5].",
            parameters.getDoubleOrDefault("overlap", 0.0d))
        .validate(
            value -> (int) value > 0,
            "window has to be a positive integer.",
            parameters.getIntOrDefault("window", DEFAULT_WINDOW))
        .validate(
            value -> (int) value > 0,
            "step has to be a positive integer.",
            parameters.getIntOrDefault(
                "step", parameters.getIntOrDefault("window", DEFAULT_WINDOW)))
        .validate(
            value -> (int) value > 0,
            "max_points has to be a positive integer.",
            parameters.getIntOrDefault("max_points", DEFAULT_MAX_POINTS))
        .validate(
            args -> (int) args[0] >= (int) args[1],
            "window has to be greater than or equal to l_max.",
            parameters.getIntOrDefault("window", DEFAULT_WINDOW),
            parameters.getIntOrDefault(L_MAX, -1))
        .validate(
            args -> (int) args[0] <= (int) args[1],
            "window has to be less than or equal to max_points.",
            parameters.getIntOrDefault("window", DEFAULT_WINDOW),
            parameters.getIntOrDefault("max_points", DEFAULT_MAX_POINTS))
        .validate(
            value -> "json".equalsIgnoreCase((String) value),
            "Only json output is supported now.",
            parameters.getStringOrDefault("output", "json"));
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {
    lMin = parameters.getInt(L_MIN);
    lMax = parameters.getInt(L_MAX);
    warping = parameters.getBooleanOrDefault("warping", true);
    rho = parameters.getDoubleOrDefault("rho", warping ? 0.8d : 0.5d);
    nb = parameters.getIntOrDefault("nb", DEFAULT_NB);
    overlap = parameters.getDoubleOrDefault("overlap", 0.0d);
    normalize = parameters.getBooleanOrDefault("normalize", true);
    equalWeightDimensions = parameters.getBooleanOrDefault("equal_weight_dims", false);
    window = parameters.getIntOrDefault("window", DEFAULT_WINDOW);
    step = parameters.getIntOrDefault("step", window);
    maxPoints = parameters.getIntOrDefault("max_points", DEFAULT_MAX_POINTS);
    dimension = parameters.getChildExpressionsSize();
    emittedTimes = new HashSet<>();
    configurations
        .setAccessStrategy(new SlidingSizeWindowAccessStrategy(window, step))
        .setOutputDataType(Type.TEXT);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    if (rowWindow.windowSize() < lMin) {
      return;
    }
    long[] rawTimes = new long[rowWindow.windowSize()];
    double[][] rawValues = new double[rowWindow.windowSize()][dimension];
    int validRows = collectValidRows(rowWindow, rawTimes, rawValues);
    if (validRows < lMin) {
      return;
    }
    if (validRows > maxPoints) {
      throw new UDFException(
          String.format(
              "LoCoMotif input window has %d valid rows, exceeding max_points.", validRows));
    }

    long[] times = Arrays.copyOf(rawTimes, validRows);
    double[][] values = new double[validRows][dimension];
    for (int i = 0; i < validRows; i++) {
      System.arraycopy(rawValues[i], 0, values[i], 0, dimension);
    }
    if (normalize) {
      values = LoCoMotifUtils.zNormalize(values);
    }

    try {
      LoCoMotif loCoMotif =
          new LoCoMotif(values, lMin, lMax, rho, nb, overlap, warping, equalWeightDimensions);
      List<MotifSet> motifSets = loCoMotif.findMotifSets();
      for (int i = 0; i < motifSets.size(); i++) {
        MotifSet motifSet = motifSets.get(i);
        long outputTime = nextOutputTime(times[motifSet.getRepresentative().getBeginIndex()]);
        String json =
            LoCoMotifUtils.toJson(
                motifSet, times, rowWindow.windowStartTime(), rowWindow.windowEndTime(), i);
        collector.putString(outputTime, json);
      }
    } catch (RuntimeException e) {
      throw new UDFException("Failed to calculate LoCoMotif.", e);
    }
  }

  private int collectValidRows(RowWindow rowWindow, long[] times, double[][] values)
      throws Exception {
    int validRows = 0;
    for (int rowIndex = 0; rowIndex < rowWindow.windowSize(); rowIndex++) {
      Row row = rowWindow.getRow(rowIndex);
      boolean hasNull = false;
      for (int column = 0; column < dimension; column++) {
        if (row.isNull(column)) {
          hasNull = true;
          break;
        }
      }
      if (hasNull) {
        continue;
      }
      times[validRows] = row.getTime();
      for (int column = 0; column < dimension; column++) {
        values[validRows][column] = Util.getValueAsDouble(row, column);
      }
      validRows++;
    }
    return validRows;
  }

  private long nextOutputTime(long preferredTime) {
    long outputTime = preferredTime;
    while (emittedTimes.contains(outputTime)) {
      outputTime++;
    }
    emittedTimes.add(outputTime);
    return outputTime;
  }
}
