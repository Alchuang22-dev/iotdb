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

package org.apache.iotdb.db.queryengine.plan.relational.metadata;

import org.apache.iotdb.commons.schema.table.TreeViewSchema;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TreeDeviceViewSchema extends TableSchema {
  public TreeDeviceViewSchema(
      final String tableName, final List<ColumnSchema> columns, final Map<String, String> props) {
    super(tableName, columns);
    setProps(props);
  }

  public Map<String, String> getColumn2OriginalNameMap() {
    return columns.stream()
        .filter(
            columnSchema ->
                Objects.nonNull(columnSchema.getProps())
                    && columnSchema.getProps().containsKey(TreeViewSchema.ORIGINAL_NAME))
        .collect(
            Collectors.toMap(
                ColumnSchema::getName,
                columnSchema -> columnSchema.getProps().get(TreeViewSchema.ORIGINAL_NAME)));
  }
}
