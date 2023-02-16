/*
 * Copyright © 2015-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.common.db;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.db.recordbuilder.CommonRecordBuilder;
import io.cdap.plugin.common.db.recordbuilder.RecordBuilder;
import io.cdap.plugin.common.db.recordwriter.ColumnType;
import io.cdap.plugin.common.db.recordwriter.CommonRecordWriter;
import io.cdap.plugin.common.db.recordwriter.RecordWriter;
import io.cdap.plugin.common.db.schemareader.CommonSchemaReader;
import io.cdap.plugin.common.db.schemareader.SchemaReader;
import io.cdap.plugin.common.db.util.Lazy;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Writable class for DB Source/Sink
 *
 * @see org.apache.hadoop.mapreduce.lib.db.DBInputFormat DBInputFormat
 * @see org.apache.hadoop.mapreduce.lib.db.DBOutputFormat DBOutputFormat
 * @see DBWritable DBWritable
 */
public class DBRecord implements Writable, DBWritable, Configurable, DataSizeReporter {
  private StructuredRecord record;
  private Configuration conf;
  private long bytesWritten;
  private long bytesRead;

  private final Lazy<Schema> schema = new Lazy<>(this::computeSchema);

  /**
   * Need to cache {@link ResultSetMetaData} of the record for use during writing to a table.
   * This is because we cannot rely on JDBC drivers to properly set metadata in the {@link PreparedStatement}
   * passed to the #write method in this class.
   */
  private List<ColumnType> columnTypes;

  /**
   * Used to construct a DBRecord from a StructuredRecord in the ETL Pipeline
   *
   * @param record the {@link StructuredRecord} to construct the {@link DBRecord} from
   */
  public DBRecord(StructuredRecord record, List<ColumnType> columnTypes) {
    this.record = record;
    this.columnTypes = columnTypes;
  }

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public DBRecord() {
  }

  public void readFields(DataInput in) throws IOException {
    // no-op, since we may never need to support a scenario where you read a DBRecord from a non-RDBMS source
  }

  /**
   * @return the {@link StructuredRecord} contained in this object
   */
  public StructuredRecord getRecord() {
    return record;
  }

  /**
   * @return the size of data written.
   */
  public long getBytesWritten() {
    return bytesWritten;
  }

  /**
   * @return the size of data read.
   */
  public long getBytesRead() {
    return bytesRead;
  }

  /**
   * Builds the {@link #record} using the specified {@link ResultSet}
   *
   * @param resultSet the {@link ResultSet} to build the {@link StructuredRecord} from
   */
  public void readFields(ResultSet resultSet) throws SQLException {
    bytesRead = 0;

    SchemaReader schemaReader = getSchemaReader();
    if (schemaReader == null) {
      throw new IllegalStateException("No Schema Reader found for extracting field schema from ResultSet.");
    }

    String patternToReplace = conf.get(DBUtils.PATTERN_TO_REPLACE);
    String replaceWith = conf.get(DBUtils.REPLACE_WITH);
    List<Schema.Field> newSchemaFields = schemaReader.getSchemaFields(resultSet, patternToReplace, replaceWith);

    RecordBuilder recordBuilder = getRecordBuilder();
    if (recordBuilder == null) {
      throw new IllegalStateException("No Record Builder found for creating Records from ResultSet.");
    }

    // Filter the schema fields based on the fields which are present in the override schema
    List<Schema.Field> schemaFields = DBUtils.getSchemaFields(Schema.recordOf("resultSet", newSchemaFields),
                                                              getSchema());

    Schema schema = Schema.recordOf("dbRecord", schemaFields);
    record = recordBuilder.getRecordBuilder(resultSet, schema).build();
    bytesRead += recordBuilder.getBytesRead();
  }

  protected Schema getSchema() {
    return schema.getOrCompute();
  }

  private Schema computeSchema() {
    String schemaStr = conf.get(DBUtils.OVERRIDE_SCHEMA);
    if (!Strings.isNullOrEmpty(schemaStr)) {
      try {
        return Schema.parseJson(schemaStr);
      } catch (IOException e) {
        throw new IllegalStateException(String.format("Unable to parse schema string %s", schemaStr), e);
      }
    } else {
      return null;
    }
  }

  /**
   * Returns the CommonSchemaReader instance. Extending classes should override this method to return specific
   * SchemaReader in order to create Schema object in a customised way.
   *
   * @return Schema Reader to use
   */
  protected SchemaReader getSchemaReader() {
    return new CommonSchemaReader();
  }

  /**
   * Returns the CommonRecordBuilder instance. Extending classes should override this method to return specific
   * RecordBuilder in order to handle ResultSet in a customised way.
   *
   * @return Record Reader Helper to use
   */
  protected RecordBuilder getRecordBuilder() {
    return new CommonRecordBuilder();
  }

  /**
   * Returns the CommonRecordWriter instance. Extending classes should override this method to return specific
   * RecordWriter in order to write a StructuredRecord in a customised way.
   * @return
   */
  protected RecordWriter getRecordWriter() {
    return new CommonRecordWriter();
  }

  public void write(DataOutput out) throws IOException {
    Schema recordSchema = record.getSchema();
    List<Schema.Field> schemaFields = recordSchema.getFields();
    for (Schema.Field field : schemaFields) {
      writeToDataOut(out, field);
    }
  }

  /**
   * Writes the {@link #record} to the specified {@link PreparedStatement}
   *
   * @param stmt the {@link PreparedStatement} to write the {@link StructuredRecord} to
   */
  public void write(PreparedStatement stmt) throws SQLException {
    bytesWritten = 0;
    RecordWriter recordWriter = getRecordWriter();
    recordWriter.write(stmt, record, columnTypes);
    bytesWritten += recordWriter.getBytesWritten();
  }

  private Schema getNonNullableSchema(Schema.Field field) {
    Schema schema = field.getSchema();
    if (field.getSchema().isNullable()) {
      schema = field.getSchema().getNonNullable();
    }
    Preconditions.checkArgument(schema.getType().isSimpleType(),
                                "Only simple types are supported (boolean, int, long, float, double, string, bytes) " +
                                  "for writing a DBRecord, but found '%s' as the type for column '%s'. Please " +
                                  "remove this column or transform it to a simple type.", schema.getType(),
                                field.getName());
    return schema;
  }

  private void writeToDataOut(DataOutput out, Schema.Field field) throws IOException {
    Schema fieldSchema = getNonNullableSchema(field);
    Schema.Type fieldType = fieldSchema.getType();
    Object fieldValue = record.get(field.getName());

    if (fieldValue == null) {
      return;
    }

    switch (fieldType) {
      case NULL:
        break;
      case STRING:
        // write string appropriately
        out.writeUTF((String) fieldValue);
        break;
      case BOOLEAN:
        out.writeBoolean((Boolean) fieldValue);
        break;
      case INT:
        // write short or int appropriately
        out.writeInt((Integer) fieldValue);
        break;
      case LONG:
        // write date, timestamp or long appropriately
        out.writeLong((Long) fieldValue);
        break;
      case FLOAT:
        // both real and float are set with the same method on prepared statement
        out.writeFloat((Float) fieldValue);
        break;
      case DOUBLE:
        out.writeDouble((Double) fieldValue);
        break;
      case BYTES:
        out.write((byte[]) fieldValue);
        break;
      default:
        throw new IOException(String.format("Column %s with value %s has an unsupported datatype %s",
          field.getName(), fieldValue, fieldType));
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
