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

package org.apache.iotdb.commons.schema.table;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.runtime.SchemaExecutionException;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchemaUtil;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.concurrent.ThreadSafe;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.commons.conf.IoTDBConstant.TTL_INFINITE;

@ThreadSafe
public class TsTable {

  public static final String TIME_COLUMN_NAME = "time";
  public static final String COMMENT_KEY = "__comment";
  private static final TimeColumnSchema TIME_COLUMN_SCHEMA =
      new TimeColumnSchema(TIME_COLUMN_NAME, TSDataType.TIMESTAMP);

  public static final String TTL_PROPERTY = "ttl";
  public static final Set<String> TABLE_ALLOWED_PROPERTIES = Collections.singleton(TTL_PROPERTY);
  private String tableName;

  private final Map<String, TsTableColumnSchema> columnSchemaMap = new LinkedHashMap<>();
  private final Map<String, Integer> tagColumnIndexMap = new HashMap<>();

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  private Map<String, String> props = null;

  // Cache, avoid string parsing
  private transient long ttlValue = Long.MIN_VALUE;
  private transient int tagNums = 0;
  private transient int fieldNum = 0;

  public TsTable(final String tableName) {
    this.tableName = tableName;
    columnSchemaMap.put(TIME_COLUMN_NAME, TIME_COLUMN_SCHEMA);
  }

  // This interface is used by InformationSchema table, so time column is not necessary
  public TsTable(String tableName, ImmutableList<TsTableColumnSchema> columnSchemas) {
    this.tableName = tableName;
    columnSchemas.forEach(
        columnSchema -> columnSchemaMap.put(columnSchema.getColumnName(), columnSchema));
  }

  public String getTableName() {
    return tableName;
  }

  public TsTableColumnSchema getColumnSchema(final String columnName) {
    readWriteLock.readLock().lock();
    try {
      return columnSchemaMap.get(columnName);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public int getTagColumnOrdinal(final String columnName) {
    readWriteLock.readLock().lock();
    try {
      return tagColumnIndexMap.getOrDefault(columnName.toLowerCase(), -1);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public List<TsTableColumnSchema> getTagColumnSchemaList() {
    readWriteLock.readLock().lock();
    try {
      final List<TsTableColumnSchema> tagColumnSchemaList = new ArrayList<>();
      for (final TsTableColumnSchema columnSchema : columnSchemaMap.values()) {
        if (TsTableColumnCategory.TAG.equals(columnSchema.getColumnCategory())) {
          tagColumnSchemaList.add(columnSchema);
        }
      }
      return tagColumnSchemaList;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  // Currently only supports device view
  public void renameTable(final String newName) {
    readWriteLock.writeLock().lock();
    try {
      tableName = newName;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void addColumnSchema(final TsTableColumnSchema columnSchema) {
    readWriteLock.writeLock().lock();
    try {
      columnSchemaMap.put(columnSchema.getColumnName(), columnSchema);
      if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.TAG)) {
        tagNums++;
        tagColumnIndexMap.put(columnSchema.getColumnName(), tagNums - 1);
      } else if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.FIELD)) {
        fieldNum++;
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void renameColumnSchema(final String oldName, final String newName) {
    readWriteLock.writeLock().lock();
    try {
      // Ensures idempotency
      if (columnSchemaMap.containsKey(oldName)) {
        final TsTableColumnSchema schema = columnSchemaMap.remove(oldName);
        final Map<String, String> oldProps = schema.getProps();
        oldProps.computeIfAbsent(TreeViewSchema.ORIGINAL_NAME, k -> schema.getColumnName());
        switch (schema.getColumnCategory()) {
          case TAG:
            columnSchemaMap.put(
                newName, new TagColumnSchema(newName, schema.getDataType(), oldProps));
            break;
          case FIELD:
            columnSchemaMap.put(
                newName,
                new FieldColumnSchema(
                    newName,
                    schema.getDataType(),
                    ((FieldColumnSchema) schema).getEncoding(),
                    ((FieldColumnSchema) schema).getCompressor(),
                    oldProps));
            break;
          case ATTRIBUTE:
            columnSchemaMap.put(
                newName, new AttributeColumnSchema(newName, schema.getDataType(), oldProps));
            break;
          case TIME:
          default:
            // Do nothing
            columnSchemaMap.put(oldName, schema);
        }
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void removeColumnSchema(final String columnName) {
    readWriteLock.writeLock().lock();
    try {
      final TsTableColumnSchema columnSchema = columnSchemaMap.get(columnName);
      if (columnSchema != null
          && columnSchema.getColumnCategory().equals(TsTableColumnCategory.TAG)) {
        throw new SchemaExecutionException("Cannot remove an tag column: " + columnName);
      } else if (columnSchema != null) {
        columnSchemaMap.remove(columnName);
        if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.FIELD)) {
          fieldNum--;
        }
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public int getColumnNum() {
    readWriteLock.readLock().lock();
    try {
      return columnSchemaMap.size();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public int getTagNum() {
    readWriteLock.readLock().lock();
    try {
      return tagNums;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public int getFieldNum() {
    readWriteLock.readLock().lock();
    try {
      return fieldNum;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public List<TsTableColumnSchema> getColumnList() {
    readWriteLock.readLock().lock();
    try {
      return new ArrayList<>(columnSchemaMap.values());
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  // This shall only be called on DataNode, where the tsTable is replaced completely thus an old
  // cache won't pollute the newest value
  public long getCachedTableTTL() {
    // Cache for performance
    if (ttlValue < 0) {
      ttlValue = getTableTTL();
    }
    return ttlValue;
  }

  public long getTableTTL() {
    final Optional<String> ttl = getPropValue(TTL_PROPERTY);
    return ttl.isPresent() && !ttl.get().equalsIgnoreCase(TTL_INFINITE)
        ? CommonDateTimeUtils.convertMilliTimeWithPrecision(
            Long.parseLong(ttl.get()),
            CommonDescriptor.getInstance().getConfig().getTimestampPrecision())
        : Long.MAX_VALUE;
  }

  public Map<String, String> getProps() {
    readWriteLock.readLock().lock();
    try {
      return props;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public boolean containsPropWithoutLock(final String propKey) {
    return props != null && props.containsKey(propKey);
  }

  public Optional<String> getPropValue(final String propKey) {
    readWriteLock.readLock().lock();
    try {
      return props != null && props.containsKey(propKey)
          ? Optional.of(props.get(propKey))
          : Optional.empty();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public void addProp(final String key, final String value) {
    readWriteLock.writeLock().lock();
    try {
      if (props == null) {
        props = new HashMap<>();
      }
      props.put(key, value);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void removeProp(final String key) {
    readWriteLock.writeLock().lock();
    try {
      if (props == null) {
        return;
      }
      props.remove(key);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public byte[] serialize() {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      serialize(stream);
    } catch (IOException ignored) {
      // won't happen
    }
    return stream.toByteArray();
  }

  public void serialize(final OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(tableName, stream);
    ReadWriteIOUtils.write(columnSchemaMap.size(), stream);
    for (final TsTableColumnSchema columnSchema : columnSchemaMap.values()) {
      TsTableColumnSchemaUtil.serialize(columnSchema, stream);
    }
    ReadWriteIOUtils.write(props, stream);
  }

  public static TsTable deserialize(InputStream inputStream) throws IOException {
    String name = ReadWriteIOUtils.readString(inputStream);
    TsTable table = new TsTable(name);
    int columnNum = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < columnNum; i++) {
      table.addColumnSchema(TsTableColumnSchemaUtil.deserialize(inputStream));
    }
    table.props = ReadWriteIOUtils.readMap(inputStream);
    return table;
  }

  public static TsTable deserialize(ByteBuffer buffer) {
    String name = ReadWriteIOUtils.readString(buffer);
    TsTable table = new TsTable(name);
    int columnNum = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < columnNum; i++) {
      table.addColumnSchema(TsTableColumnSchemaUtil.deserialize(buffer));
    }
    table.props = ReadWriteIOUtils.readMap(buffer);
    return table;
  }

  public void setProps(Map<String, String> props) {
    readWriteLock.writeLock().lock();
    try {
      this.props = props;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName);
  }

  @Override
  public String toString() {
    return "TsTable{"
        + "tableName='"
        + tableName
        + '\''
        + ", columnSchemaMap="
        + columnSchemaMap
        + ", props="
        + props
        + '}';
  }
}
