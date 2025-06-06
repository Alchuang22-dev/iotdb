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

package org.apache.iotdb.confignode.consensus.request.write.table;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SetTableCommentPlan extends AbstractTablePlan {
  private String comment;

  public SetTableCommentPlan(final ConfigPhysicalPlanType type) {
    super(type);
  }

  public SetTableCommentPlan(final String database, final String tableName, final String comment) {
    this(ConfigPhysicalPlanType.SetTableComment, database, tableName, comment);
  }

  protected SetTableCommentPlan(
      final ConfigPhysicalPlanType type,
      final String database,
      final String tableName,
      final String comment) {
    super(type, database, tableName);
    this.comment = comment;
  }

  public String getComment() {
    return comment;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    super.serializeImpl(stream);
    ReadWriteIOUtils.write(comment, stream);
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    super.deserializeImpl(buffer);
    this.comment = ReadWriteIOUtils.readString(buffer);
  }
}
