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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DataPartitionPolicyTableTest {

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final int SERIES_SLOT_NUM = CONF.getSeriesSlotNum();

  @Test
  public void testUpdateDataAllotTable() {
    DataPartitionPolicyTable dataPartitionPolicyTable = new DataPartitionPolicyTable();
    List<TConsensusGroupId> dataRegionGroups = new ArrayList<>();

    // Test 1: construct DataAllotTable from scratch
    TConsensusGroupId group1 = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    dataRegionGroups.add(group1);
    dataPartitionPolicyTable.reBalanceDataPartitionPolicy(dataRegionGroups);
    for (int i = 0; i < SERIES_SLOT_NUM; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      // All SeriesPartitionSlots belong to group1
      Assert.assertEquals(
          group1,
          dataPartitionPolicyTable.getRegionGroupIdOrActivateIfNecessary(seriesPartitionSlot));
    }

    // Test2: extend DataRegionGroups
    Map<TSeriesPartitionSlot, TConsensusGroupId> lastDataAllotTable = new HashMap<>();
    dataRegionGroups.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, 2));
    dataRegionGroups.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, 3));
    dataPartitionPolicyTable.reBalanceDataPartitionPolicy(dataRegionGroups);
    int mu = SERIES_SLOT_NUM / 3;
    Map<TConsensusGroupId, AtomicInteger> counter = new HashMap<>();
    for (int i = 0; i < SERIES_SLOT_NUM; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      TConsensusGroupId groupId =
          dataPartitionPolicyTable.getRegionGroupIdOrActivateIfNecessary(seriesPartitionSlot);
      lastDataAllotTable.put(seriesPartitionSlot, groupId);
      counter.computeIfAbsent(groupId, empty -> new AtomicInteger(0)).incrementAndGet();
    }
    // All DataRegionGroups divide SeriesPartitionSlots evenly
    for (Map.Entry<TConsensusGroupId, AtomicInteger> counterEntry : counter.entrySet()) {
      Assert.assertTrue(Math.abs(counterEntry.getValue().get() - mu) <= 1);
    }

    // Test 3: extend DataRegionGroups while inherit as much SeriesPartitionSlots as possible
    dataRegionGroups.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, 4));
    dataRegionGroups.add(new TConsensusGroupId(TConsensusGroupType.DataRegion, 5));
    dataPartitionPolicyTable.reBalanceDataPartitionPolicy(dataRegionGroups);
    Map<TConsensusGroupId, AtomicInteger> unchangedSlots = new HashMap<>();
    mu = SERIES_SLOT_NUM / 5;
    counter.clear();
    for (int i = 0; i < SERIES_SLOT_NUM; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      TConsensusGroupId groupId =
          dataPartitionPolicyTable.getRegionGroupIdOrActivateIfNecessary(seriesPartitionSlot);
      counter.computeIfAbsent(groupId, empty -> new AtomicInteger(0)).incrementAndGet();
      if (groupId.getId() < 4) {
        // Most of SeriesPartitionSlots in the first three DataRegionGroups should remain unchanged
        Assert.assertEquals(lastDataAllotTable.get(seriesPartitionSlot), groupId);
        unchangedSlots.computeIfAbsent(groupId, empty -> new AtomicInteger(0)).incrementAndGet();
      }
    }
    // All DataRegionGroups divide SeriesPartitionSlots evenly
    for (Map.Entry<TConsensusGroupId, AtomicInteger> counterEntry : counter.entrySet()) {
      Assert.assertTrue(Math.abs(counterEntry.getValue().get() - mu) <= 1);
    }
    // Most of SeriesPartitionSlots in the first three DataRegionGroups should remain unchanged
    for (Map.Entry<TConsensusGroupId, AtomicInteger> counterEntry : unchangedSlots.entrySet()) {
      Assert.assertEquals(mu, counterEntry.getValue().get());
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<TConsensusGroupId, Integer> getSeriesCounter(DataPartitionPolicyTable t)
      throws Exception {
    Field f = DataPartitionPolicyTable.class.getDeclaredField("seriesPartitionSlotCounter");
    f.setAccessible(true);
    Object raw = f.get(t);
    return (Map<TConsensusGroupId, Integer>) raw;
  }

  /** 便捷设置计数：以 (id1, c1, id2, c2, ...) 的形式传入 */
  private static void setCounts(DataPartitionPolicyTable table, int... idsInOrderAndCounts)
      throws Exception {
    Map<TConsensusGroupId, Integer> counter = getSeriesCounter(table);
    counter.clear();
    for (int i = 0; i + 1 < idsInOrderAndCounts.length; i += 2) {
      int id = idsInOrderAndCounts[i];
      int c = idsInOrderAndCounts[i + 1];
      counter.put(new TConsensusGroupId(TConsensusGroupType.DataRegion, id), c);
    }
  }

  private static TConsensusGroupId gid(int id) {
    return new TConsensusGroupId(TConsensusGroupType.DataRegion, id);
  }

  @Test
  public void testNewFunction_RoundRobinWhenAllCountsEqual() throws Exception {
    DataPartitionPolicyTable table = new DataPartitionPolicyTable();
    setCounts(table, 1, 0, 2, 0, 3, 0);

    List<TConsensusGroupId> seq = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      seq.add(table.newFunction()); // 期望 1,2,3,1,2,3
    }
    Assert.assertEquals(Arrays.asList(gid(1), gid(2), gid(3), gid(1), gid(2), gid(3)), seq);
  }

  @Test
  public void testNewFunction_NearMinWindow() throws Exception {
    DataPartitionPolicyTable table = new DataPartitionPolicyTable();
    setCounts(table, 1, 0, 2, 0, 3, 1, 4, 5);

    List<TConsensusGroupId> seq = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      seq.add(table.newFunction());
    }
    Assert.assertEquals(Arrays.asList(gid(1), gid(2), gid(1), gid(2)), seq);
  }

  @Test
  public void testNewFunction_GreedyFallbackWhenNoNearMin() throws Exception {
    DataPartitionPolicyTable table = new DataPartitionPolicyTable();
    setCounts(table, 1, 0, 2, 5, 3, 10);

    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(gid(1), table.newFunction());
    }
  }

  @Test
  public void testNewFunction_DynamicExitFromNearMin() throws Exception {
    DataPartitionPolicyTable table = new DataPartitionPolicyTable();
    setCounts(table, 1, 0, 2, 0, 3, 0);

    Assert.assertEquals(gid(1), table.newFunction());

    Map<TConsensusGroupId, Integer> counter = getSeriesCounter(table);
    counter.put(gid(1), counter.get(gid(1)) + 1);

    Assert.assertEquals(gid(2), table.newFunction());
    Assert.assertEquals(gid(3), table.newFunction());

    counter.put(gid(2), counter.get(gid(2)) + 1);
    Assert.assertEquals(gid(3), table.newFunction());
  }

  @Test
  public void testNewFunction_StableOrderingOfNearMin() throws Exception {
    DataPartitionPolicyTable table = new DataPartitionPolicyTable();
    setCounts(table, 3, 0, 2, 0, 1, 0);

    List<TConsensusGroupId> seq = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      seq.add(table.newFunction());
    }
    Assert.assertEquals(Arrays.asList(gid(1), gid(2), gid(3), gid(1), gid(2), gid(3)), seq);
  }
}
