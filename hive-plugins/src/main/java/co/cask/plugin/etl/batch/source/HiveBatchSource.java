/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.plugin.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.plugin.etl.batch.HiveConfig;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.List;

/**
 * Batch source for Hive.
 */
@Plugin(type = "batchsource")
@Name("Hive")
@Description("Read from an Hive table in batch")
public class HiveBatchSource extends BatchSource<WritableComparable, HCatRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(HiveBatchSource.class);
  private static final Gson GSON = new Gson();

  private HiveConfig config;
  private HCatSchema hiveSchema;
  private Schema schema;

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    LOG.trace("Hadoop version: {}", VersionInfo.getVersion());
    Job job = context.getHadoopJob();
    Configuration configuration = job.getConfiguration();
    job.setInputFormatClass(HCatInputFormat.class);
    configuration.set("hive.metastore.uris", config.metaStoreURI);
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      HCatInputFormat.setInput(job, config.dbName, config.tableName);
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
    HCatSchema hiveSchema = HCatInputFormat.getTableSchema(configuration);
    List<HCatFieldSchema> fields = hiveSchema.getFields();
    LOG.info("In prepareRun the fields are {}", fields);
    configuration.set("source.hive.schema", GSON.toJson(fields));
  }

    @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    Configuration hConf = new Configuration();
//    HCatSchema hiveSchema = HCatInputFormat.getTableSchema(hConf);
    List<HCatFieldSchema> fields;
    try (FileInputStream fis = new FileInputStream("job.xml")) {
      hConf.addResource(fis);

      fields = GSON.fromJson(hConf.get("source.hive.schema"), new TypeToken<List<HCatFieldSchema>>() {
      }.getType());
    }
    System.out.println("########################### fields = " + GSON.toJson(fields));
//    hiveSchema = new HCatSchema(fields);
//    schema = convertSchema(hiveSchema);
  }


  @Override
  public void transform(KeyValue<WritableComparable, HCatRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
    LOG.info("The HCatRedord is: {}", input.getValue().toString());
//    LOG.info("The Hive Schema is: {}", hiveSchema.toString());
//    LOG.info("The CDAP Schema is: {}", schema.toString());
  }

  private static Schema convertSchema(HCatSchema hiveSchema) {
    List<Schema.Field> fields = Lists.newArrayList();
    for (HCatFieldSchema field : hiveSchema.getFields()) {
      String name = field.getName();
      if (field.isComplex()) {
        throw new IllegalArgumentException(String.format(
          "Table schema contains field '%s' with complex type %s. Only primitive types are supported.",
          name, field.getTypeString()));
      }
      PrimitiveTypeInfo typeInfo = field.getTypeInfo();
      fields.add(Schema.Field.of(name, Schema.of(getType(name, typeInfo.getPrimitiveCategory()))));
    }
    return Schema.recordOf("record", fields);
  }

  private static Schema.Type getType(String name, PrimitiveObjectInspector.PrimitiveCategory category) {
    switch (category) {
      case BOOLEAN:
        return Schema.Type.BOOLEAN;
      case BYTE:
      case CHAR:
      case SHORT:
      case INT:
        return Schema.Type.INT;
      case LONG:
        return Schema.Type.LONG;
      case FLOAT:
        return Schema.Type.FLOAT;
      case DOUBLE:
        return Schema.Type.DOUBLE;
      case STRING:
      case VARCHAR:
        return Schema.Type.STRING;
      case BINARY:
        return Schema.Type.BYTES;
      case VOID:
      case DATE:
      case TIMESTAMP:
      case DECIMAL:
      case UNKNOWN:
      default:
        throw new IllegalArgumentException(String.format(
          "Table schema contains field '%s' with unsupported type %s", name, category.name()));
    }
  }
}
