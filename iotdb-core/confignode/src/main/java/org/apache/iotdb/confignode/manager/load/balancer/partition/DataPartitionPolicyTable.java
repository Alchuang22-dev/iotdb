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

package org.apache.iotdb.confignode.manager.load.balancer.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.structure.BalanceTreeMap;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class DataPartitionPolicyTable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataPartitionPolicyTable.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final int SERIES_SLOT_NUM = CONF.getSeriesSlotNum();

  private final ReentrantLock dataAllotTableLock;

  // Map<SeriesPartitionSlot, RegionGroupId>
  // The optimal allocation of SeriesSlots to RegionGroups
  private final Map<TSeriesPartitionSlot, TConsensusGroupId> dataAllotMap;

  // Map<RegionGroupId, SeriesPartitionSlot Count>
  // The number of SeriesSlots allocated to each RegionGroup in dataAllotMap
  private final BalanceTreeMap<TConsensusGroupId, Integer> seriesPartitionSlotCounter;

  private final java.util.concurrent.atomic.AtomicInteger rrCursor =
      new java.util.concurrent.atomic.AtomicInteger(0);

  public DataPartitionPolicyTable() {
    this.dataAllotTableLock = new ReentrantLock();
    this.dataAllotMap = new HashMap<>();
    this.seriesPartitionSlotCounter = new BalanceTreeMap<>();
  }

  private static int btGetOrDefault(
      BalanceTreeMap<TConsensusGroupId, Integer> map, TConsensusGroupId key, int defaultValue) {
    if (map == null) return defaultValue;
    Integer v = map.get(key);
    return (v != null) ? v.intValue() : defaultValue;
  }

  public TConsensusGroupId newFunction() {
    if (seriesPartitionSlotCounter == null || seriesPartitionSlotCounter.isEmpty()) {
      return null;
    }

    java.util.List<TConsensusGroupId> candidates =
        new java.util.ArrayList<>(seriesPartitionSlotCounter.keySet());

    int cMin = Integer.MAX_VALUE;
    int cMax = Integer.MIN_VALUE;
    java.util.Map<TConsensusGroupId, Integer> countMap = new java.util.HashMap<>(candidates.size());

    for (TConsensusGroupId gid : candidates) {
      int c = btGetOrDefault(seriesPartitionSlotCounter, gid, 0);
      countMap.put(gid, c);
      if (c < cMin) cMin = c;
      if (c > cMax) cMax = c;
    }

    final int denom = Math.max(1, cMax - cMin);
    java.util.Map<TConsensusGroupId, Double> loadMap = new java.util.HashMap<>(candidates.size());
    double minLoad = Double.POSITIVE_INFINITY;
    for (TConsensusGroupId gid : candidates) {
      double load = (countMap.get(gid) - cMin) * 1.0 / denom;
      loadMap.put(gid, load);
      if (load < minLoad) minLoad = load;
    }

    final double epsilon = 0.05;
    final double threshold = minLoad + epsilon;

    java.util.List<TConsensusGroupId> nearMin = new java.util.ArrayList<>();
    for (TConsensusGroupId gid : candidates) {
      if (loadMap.get(gid) <= threshold) {
        nearMin.add(gid);
      }
    }

    nearMin.sort(java.util.Comparator.comparingInt(TConsensusGroupId::getId));

    if (!nearMin.isEmpty()) {
      int idx = Math.floorMod(rrCursor.getAndIncrement(), nearMin.size());
      return nearMin.get(idx);
    }

    TConsensusGroupId best = candidates.get(0);
    double bestLoad = loadMap.get(best);
    for (int i = 1; i < candidates.size(); i++) {
      TConsensusGroupId gid = candidates.get(i);
      double l = loadMap.get(gid);
      if (l < bestLoad) {
        bestLoad = l;
        best = gid;
      }
    }
    return best;
  }

  /**
   * Get or activate the specified SeriesPartitionSlot in dataAllotMap.
   *
   * @param seriesPartitionSlot The specified SeriesPartitionSlot
   * @return The RegionGroupId of the specified SeriesPartitionSlot, activate when its empty yet
   */
  public TConsensusGroupId getRegionGroupIdOrActivateIfNecessary(
      TSeriesPartitionSlot seriesPartitionSlot) {
    if (dataAllotMap.containsKey(seriesPartitionSlot)) {
      return dataAllotMap.get(seriesPartitionSlot);
    }

    TConsensusGroupId regionGroupId = newFunction();
    ;
    dataAllotMap.put(seriesPartitionSlot, regionGroupId);
    seriesPartitionSlotCounter.put(
        regionGroupId, seriesPartitionSlotCounter.get(regionGroupId) + 1);
    LOGGER.info(
        "[ActivateDataAllotTable] Activate SeriesPartitionSlot {} "
            + "to RegionGroup {}, SeriesPartitionSlot Count: {}",
        seriesPartitionSlot,
        regionGroupId,
        seriesPartitionSlotCounter.get(regionGroupId));
    return regionGroupId;
  }

  /**
   * Re-balance the allocation of SeriesSlots to RegionGroups.
   *
   * @param dataRegionGroups All DataRegionGroups currently in the Database
   */
  public void reBalanceDataPartitionPolicy(List<TConsensusGroupId> dataRegionGroups) {
    if (dataRegionGroups.isEmpty()) {
      // No need to re-balance when there is no DataRegionGroup
      return;
    }

    dataAllotTableLock.lock();
    try {
      dataRegionGroups.forEach(
          dataRegionGroup -> {
            if (!seriesPartitionSlotCounter.containsKey(dataRegionGroup)) {
              seriesPartitionSlotCounter.put(dataRegionGroup, 0);
            }
          });

      // Enumerate all SeriesSlots randomly
      List<TSeriesPartitionSlot> seriesPartitionSlots = new ArrayList<>();
      for (int i = 0; i < SERIES_SLOT_NUM; i++) {
        seriesPartitionSlots.add(new TSeriesPartitionSlot(i));
      }
      Collections.shuffle(seriesPartitionSlots);

      int mu = SERIES_SLOT_NUM / dataRegionGroups.size();
      for (TSeriesPartitionSlot seriesPartitionSlot : seriesPartitionSlots) {
        if (!dataAllotMap.containsKey(seriesPartitionSlot)) {
          // Skip unallocated SeriesPartitionSlot
          // They will be activated when allocating DataPartition
          continue;
        }

        TConsensusGroupId regionGroupId = dataAllotMap.get(seriesPartitionSlot);
        int seriesPartitionSlotCount = seriesPartitionSlotCounter.get(regionGroupId);
        if (seriesPartitionSlotCount > mu) {
          // Remove from dataAllotMap if the number of SeriesSlots is greater than mu
          // They will be re-activated when allocating DataPartition
          dataAllotMap.remove(seriesPartitionSlot);
          seriesPartitionSlotCounter.put(regionGroupId, seriesPartitionSlotCount - 1);
        }
      }
    } finally {
      dataAllotTableLock.unlock();
    }
  }

  /** Only use this interface when init PartitionBalancer. */
  public void setDataAllotMap(Map<TSeriesPartitionSlot, TConsensusGroupId> dataAllotMap) {
    if (seriesPartitionSlotCounter.size() == 0) {
      // No need to re-balance when there is no DataRegionGroup
      return;
    }
    dataAllotTableLock.lock();
    try {
      int mu = SERIES_SLOT_NUM / seriesPartitionSlotCounter.size();
      dataAllotMap.forEach(
          (seriesPartitionSlot, regionGroupId) -> {
            if (regionGroupId != null && seriesPartitionSlotCounter.get(regionGroupId) < mu) {
              // Put into dataAllotMap only when the number of SeriesSlots
              // allocated to the RegionGroup is less than mu
              this.dataAllotMap.put(seriesPartitionSlot, regionGroupId);
              seriesPartitionSlotCounter.put(
                  regionGroupId, seriesPartitionSlotCounter.get(regionGroupId) + 1);
            }
            // Otherwise, clear this SeriesPartitionSlot and wait for re-activating
          });
    } finally {
      dataAllotTableLock.unlock();
    }
  }

  public void logDataAllotTable(String database) {
    seriesPartitionSlotCounter
        .keySet()
        .forEach(
            regionGroupId ->
                LOGGER.info(
                    "[ReBalanceDataAllotTable] Database: {}, "
                        + "RegionGroupId: {}, SeriesPartitionSlot Count: {}",
                    database,
                    regionGroupId,
                    seriesPartitionSlotCounter.get(regionGroupId)));
  }

  public void acquireLock() {
    dataAllotTableLock.lock();
  }

  public void releaseLock() {
    dataAllotTableLock.unlock();
  }
}
