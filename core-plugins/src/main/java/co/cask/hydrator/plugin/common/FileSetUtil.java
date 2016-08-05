/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.common;

import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.hydrator.common.HiveSchemaConverter;
import co.cask.hydrator.common.batch.JobUtils;
import com.google.common.base.Throwables;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.util.Map;


/**
 * Utilities for configuring file sets during pipeline configuration.
 * TODO (CDAP-6211): Why do we lower-case the schema for Parquet but not for Avro?
 */
public class FileSetUtil {

  /**
   * Configure a file set to use Parquet file format with a given schema. The schema is lower-cased, parsed
   * as an Avro schema, validated and converted into a Hive schema. The file set is configured to use
   * Parquet input and output format, and also configured for Explore to use Parquet. The schema is added
   * to the file set properties in all the different required ways:
   * <ul>
   *   <li>As a top-level dataset property;</li>
   *   <li>As the schema for the input and output format;</li>
   *   <li>As the schema of the Hive table.</li>
   * </ul>
   * @param configuredSchema the original schema configured for the table
   * @param properties a builder for the file set properties
   */
  public static void configureParquetFileSet(String configuredSchema, FileSetProperties.Builder properties) {

    // validate and parse schema as Avro, and attempt to convert it into a Hive schema
    String lowerCaseSchema = configuredSchema.toLowerCase();
    Schema avroSchema = parseAvroSchema(lowerCaseSchema, configuredSchema);
    String hiveSchema = parseHiveSchema(lowerCaseSchema, configuredSchema);

    properties
      .setInputFormat(AvroParquetInputFormat.class)
      .setOutputFormat(AvroParquetOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setExploreFormat("parquet")
      .setExploreSchema(hiveSchema.substring(1, hiveSchema.length() - 1))
      .add(DatasetProperties.SCHEMA, lowerCaseSchema);

    Job job = createJobForConfiguration();
    Configuration hConf = job.getConfiguration();
    AvroParquetInputFormat.setAvroReadSchema(job, avroSchema);
    for (Map.Entry<String, String> entry : hConf) {
      properties.setInputProperty(entry.getKey(), entry.getValue());
    }
    hConf.clear();
    AvroParquetOutputFormat.setSchema(job, avroSchema);
    for (Map.Entry<String, String> entry : hConf) {
      properties.setOutputProperty(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Configure a file set to use Avro file format with a given schema. The schema is parsed
   * as an Avro schema, validated and converted into a Hive schema. The file set is configured to use
   * Avro key input and output format, and also configured for Explore to use Avro. The schema is added
   * to the file set properties in all the different required ways:
   * <ul>
   *   <li>As a top-level dataset property;</li>
   *   <li>As the schema for the input and output format;</li>
   *   <li>As the schema of the Hive table;</li>
   *   <li>As the schema to be used by the Avro serde (which is used by Hive).</li>
   * </ul>
   * @param configuredSchema the original schema configured for the table
   * @param properties a builder for the file set properties
   */
  public static void configureAvroFileSet(String configuredSchema, FileSetProperties.Builder properties) {
    // validate and parse schema as Avro, and attempt to convert it into a Hive schema
    Schema avroSchema = parseAvroSchema(configuredSchema, configuredSchema);
    String hiveSchema = parseHiveSchema(configuredSchema, configuredSchema);

    properties
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", configuredSchema)
      .setExploreSchema(hiveSchema.substring(1, hiveSchema.length() - 1))
      .add(DatasetProperties.SCHEMA, configuredSchema);

    Job job = createJobForConfiguration();
    Configuration hConf = job.getConfiguration();
    AvroJob.setInputKeySchema(job, avroSchema);
    for (Map.Entry<String, String> entry : hConf) {
      properties.setInputProperty(entry.getKey(), entry.getValue());
    }
    hConf.clear();
    AvroJob.setOutputKeySchema(job, avroSchema);
    for (Map.Entry<String, String> entry : hConf) {
      properties.setOutputProperty(entry.getKey(), entry.getValue());
    }
  }

  /*----- private helpers ----*/

  private static Schema parseAvroSchema(String schemaString, String configuredSchema) {
    try {
      return new Schema.Parser().parse(schemaString);
    } catch (Exception e) {
      throw new IllegalArgumentException("Schema " + configuredSchema + " is invalid.", e);
    }
  }

  private static String parseHiveSchema(String schemaString, String configuredSchema) {
    try {
      return HiveSchemaConverter.toHiveSchema(co.cask.cdap.api.data.schema.Schema.parseJson(schemaString));
    } catch (UnsupportedTypeException e) {
      throw new IllegalArgumentException("Schema " + configuredSchema + " is not supported as a Hive schema.", e);
    } catch (Exception e) {
      throw new IllegalArgumentException("Schema " + configuredSchema + " is invalid.", e);
    }
  }

  private static Job createJobForConfiguration() {
    try {
      return JobUtils.createInstance();
    } catch (IOException e) {
      // Shouldn't happen
      throw Throwables.propagate(e);
    }
  }

}
