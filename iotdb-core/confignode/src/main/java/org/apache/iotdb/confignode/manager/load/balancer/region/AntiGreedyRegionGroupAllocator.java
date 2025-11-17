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

package org.apache.iotdb.confignode.manager.load.balancer.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A deliberately "anti-greedy" region allocator.
 *
 * <p>Compared with the original GreedyRegionGroupAllocator, which favors the least-loaded DataNodes
 * (fewest regions, most free disk), this implementation does the opposite:
 *
 * <ul>
 *   <li>When allocating a new RegionGroup, it always prefers the DataNodes that already host the
 *       largest number of regions (and with less free disk space if there is a tie).
 *   <li>When re-allocating replicas after removing a DataNode, it also chooses the most heavily
 *       loaded DataNodes as targets.
 * </ul>
 *
 * <p>This strategy is intentionally extreme and unbalanced, in order to highlight the impact of
 * replica selection policies in experiments. It is not intended for production usage.
 */
public class AntiGreedyRegionGroupAllocator implements IRegionGroupAllocator {

  public static final SecureRandom RANDOM = new SecureRandom();

  public AntiGreedyRegionGroupAllocator() {
    // Empty constructor
  }

  public static class DataNodeEntry implements Comparable<DataNodeEntry> {

    public int dataNodeId;

    /** Number of Regions currently hosted by this DataNode. */
    public int regionCount;

    /** Free disk space of this DataNode. */
    public double freeDiskSpace;

    /** Random tie-breaker to avoid fully deterministic ordering. */
    public int randomWeight;

    public DataNodeEntry(int dataNodeId, int regionCount, double freeDiskSpace) {
      this.dataNodeId = dataNodeId;
      this.regionCount = regionCount;
      this.freeDiskSpace = freeDiskSpace;
      this.randomWeight = RANDOM.nextInt();
    }

    @Override
    public int compareTo(DataNodeEntry other) {
      // --- HIGH-LOAD-FIRST 核心逻辑 ---
      // 1. Region 数量越多越优先（与原来的 Greedy 完全反向）
      if (this.regionCount != other.regionCount) {
        // higher regionCount comes first
        return Integer.compare(other.regionCount, this.regionCount);
      }
      // 2. 在 Region 数相同的情况下，剩余磁盘空间越少越优先（进一步制造不均衡）
      if (Double.compare(this.freeDiskSpace, other.freeDiskSpace) != 0) {
        // lower freeDiskSpace comes first
        return Double.compare(this.freeDiskSpace, other.freeDiskSpace);
      }
      // 3. 最后用随机权重打破平局
      return Integer.compare(this.randomWeight, other.randomWeight);
    }
  }

  @Override
  public TRegionReplicaSet generateOptimalRegionReplicasDistribution(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      List<TRegionReplicaSet> databaseAllocatedRegionGroups,
      int replicationFactor,
      TConsensusGroupId consensusGroupId) {

    // Build weightList ordered by number of regions allocated DESC (high-load-first)
    List<TDataNodeLocation> weightList =
        buildWeightList(availableDataNodeMap, freeDiskSpaceMap, allocatedRegionGroups);

    return new TRegionReplicaSet(
        consensusGroupId,
        weightList.stream().limit(replicationFactor).collect(Collectors.toList()));
  }

  @Override
  public Map<TConsensusGroupId, TDataNodeConfiguration> removeNodeReplicaSelect(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups,
      Map<TConsensusGroupId, String> regionDatabaseMap,
      Map<String, List<TRegionReplicaSet>> databaseAllocatedRegionGroupMap,
      Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap) {

    // 为了实验展示，这里采用一个同样“高负载优先”的极简实现：
    // 对于每个 RegionGroup：
    //   1. 统计当前各 DataNode 上的 Region 数量（基于 remainReplicasMap）
    //   2. 在可用节点中，排除已经持有该 Region 的 DataNode
    //   3. 在剩余节点中选择「Region 数量最多」的 DataNode 作为新的副本位置

    // 1. 统计所有 DataNode 的 Region 数量（基于剩余的副本）
    Map<Integer, Integer> regionCounter = new HashMap<>(availableDataNodeMap.size());
    for (TRegionReplicaSet replicaSet : remainReplicasMap.values()) {
      for (TDataNodeLocation location : replicaSet.getDataNodeLocations()) {
        regionCounter.merge(location.getDataNodeId(), 1, Integer::sum);
      }
    }

    Map<TConsensusGroupId, TDataNodeConfiguration> result = new HashMap<>();

    // 2. 对每个 RegionGroup，选择一个新的目标 DataNode
    for (Map.Entry<TConsensusGroupId, TRegionReplicaSet> entry : remainReplicasMap.entrySet()) {

      TConsensusGroupId consensusGroupId = entry.getKey();
      TRegionReplicaSet currentReplicaSet = entry.getValue();

      // 已经持有该 Region 的 DataNode，不可重复选择
      Set<Integer> existedDataNodeIds = new HashSet<>();
      for (TDataNodeLocation location : currentReplicaSet.getDataNodeLocations()) {
        existedDataNodeIds.add(location.getDataNodeId());
      }

      // 构造候选列表：所有可用节点中，排除已经持有该 Region 的节点
      List<DataNodeEntry> candidateEntries = new ArrayList<>();
      for (Map.Entry<Integer, TDataNodeConfiguration> e : availableDataNodeMap.entrySet()) {
        int dataNodeId = e.getKey();
        if (existedDataNodeIds.contains(dataNodeId)) {
          continue;
        }
        candidateEntries.add(
            new DataNodeEntry(
                dataNodeId,
                regionCounter.getOrDefault(dataNodeId, 0),
                freeDiskSpaceMap.getOrDefault(dataNodeId, 0d)));
      }

      if (candidateEntries.isEmpty()) {
        // 理论上不应该发生（可用节点数量应大于等于副本数），
        // 为了安全，这里直接跳过该 RegionGroup。
        continue;
      }

      // 使用与上面相同的 compareTo：高负载优先
      candidateEntries.sort(null);
      DataNodeEntry target = candidateEntries.get(0);

      TDataNodeConfiguration targetConfig = availableDataNodeMap.get(target.dataNodeId);
      if (targetConfig != null) {
        result.put(consensusGroupId, targetConfig);

        // 更新 regionCounter：认为这个 Region 也会落在该节点，用于后续 Region 的选择
        regionCounter.merge(target.dataNodeId, 1, Integer::sum);
      }
    }

    return result;
  }

  private List<TDataNodeLocation> buildWeightList(
      Map<Integer, TDataNodeConfiguration> availableDataNodeMap,
      Map<Integer, Double> freeDiskSpaceMap,
      List<TRegionReplicaSet> allocatedRegionGroups) {

    // Map<DataNodeId, Region count>
    Map<Integer, Integer> regionCounter = new HashMap<>(availableDataNodeMap.size());
    allocatedRegionGroups.forEach(
        regionReplicaSet ->
            regionReplicaSet
                .getDataNodeLocations()
                .forEach(
                    dataNodeLocation ->
                        regionCounter.merge(dataNodeLocation.getDataNodeId(), 1, Integer::sum)));

    // Construct entry list
    List<DataNodeEntry> entryList = new ArrayList<>();
    availableDataNodeMap.forEach(
        (dataNodeId, dataNodeConfiguration) ->
            entryList.add(
                new DataNodeEntry(
                    dataNodeId,
                    regionCounter.getOrDefault(dataNodeId, 0),
                    freeDiskSpaceMap.getOrDefault(dataNodeId, 0d))));

    // Sort entryList using HIGH-LOAD-FIRST rule and map to locations
    return entryList.stream()
        .sorted()
        .map(entry -> availableDataNodeMap.get(entry.dataNodeId).getLocation())
        .collect(Collectors.toList());
  }
}
