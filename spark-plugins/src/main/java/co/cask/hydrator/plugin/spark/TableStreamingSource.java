/*
 *
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.hydrator.plugin.spark;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.InstanceConflictException;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.hydrator.common.RowRecordTransformer;
import co.cask.hydrator.common.TimeParser;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * A StreamingSource that returns the entire contents of a Table as each micro batch and refreshes the contents
 * after some configurable amount of time
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("Table")
@Description("Returns the entire contents of a Table each batch interval, refreshing the contents at configurable " +
  "intervals. The primary use case for this plugin is to send it to a Joiner plugin to provide lookup-like " +
  "functionality.")
public class TableStreamingSource extends StreamingSource<StructuredRecord> {
  private final Conf conf;

  public TableStreamingSource(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    conf.getRefreshInterval();
    Schema schema = conf.getSchema();
    if (conf.rowField != null && schema.getField(conf.rowField) == null) {
      throw new IllegalArgumentException(String.format("rowField '%s' must be present in the schema", conf.rowField));
    }
    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
    if (!conf.containsMacro("name")) {
      pipelineConfigurer.createDataset(conf.name, Table.class.getName(), getTableProperties(schema));
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws Exception {
    JavaSparkExecutionContext cdapContext = streamingContext.getSparkExecutionContext();
    Admin admin = cdapContext.getAdmin();
    final Schema schema = conf.getSchema();
    if (!admin.datasetExists(conf.name)) {
      try {
        admin.createDataset(conf.name, "table", getTableProperties(schema));
      } catch (InstanceConflictException e) {
        // this is ok, means it was created after we checked that it didn't exist but before we were able to create it
      }
    }
    streamingContext.registerLineage(conf.name);

    JavaStreamingContext jsc = streamingContext.getSparkStreamingContext();
    return JavaDStream.fromDStream(
      new TableInputDStream(cdapContext, jsc.ssc(), conf.name,
                             conf.getRefreshInterval(), 0L, null),
      ClassTag$.MODULE$.<Tuple2<byte[], Row>>apply(Tuple2.class))
      .map(new RowToRecordFunc(schema, conf.rowField));
  }

  /**
   * Transforms a row to a record.
   */
  private static class RowToRecordFunc implements Function<Tuple2<byte[], Row>, StructuredRecord> {
    private final Schema schema;
    private final String rowField;
    private RowRecordTransformer rowRecordTransformer;

    private RowToRecordFunc(Schema schema, String rowField) {
      this.schema = schema;
      this.rowField = rowField;
    }

    @Override
    public StructuredRecord call(Tuple2<byte[], Row> row) throws Exception {
      if (rowRecordTransformer == null) {
        rowRecordTransformer = new RowRecordTransformer(schema, rowField);
      }
      return rowRecordTransformer.toRecord(row._2());
    }
  }

  private DatasetProperties getTableProperties(Schema schema) {
    TableProperties.Builder tableProperties = TableProperties.builder().setSchema(schema);
    if (conf.rowField != null) {
      tableProperties.setRowFieldName(conf.rowField);
    }
    return tableProperties.build();
  }

  /**
   * Config for the plugin
   */
  public static class Conf extends PluginConfig {
    @Macro
    @Description("The name of the CDAP Table to read from.")
    private String name;

    @Description("The schema to use when reading from the table. If the table does not already exist, one will be " +
      "created with this schema, which will allow the table to be explored through CDAP.")
    @Nullable
    private String schema;

    @Description("Optional schema field whose value is derived from the Table row instead of from a Table column. " +
      "The field name specified must be present in the schema, and must not be nullable.")
    @Nullable
    private String rowField;

    @Description("How often the table contents should be refreshed. Must be specified by a number followed by " +
      "a unit where 's', 'm', 'h', and 'd' are valid units corresponding to seconds, minutes, hours, and days. " +
      "Defaults to '1h'.")
    @Nullable
    private String refreshInterval;

    public Conf() {
      refreshInterval = "1h";
    }

    public Schema getSchema() {
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse the schema. Reason: " + e.getMessage());
      }
    }

    public long getRefreshInterval() {
      return TimeParser.parseDuration(refreshInterval);
    }
  }
}
