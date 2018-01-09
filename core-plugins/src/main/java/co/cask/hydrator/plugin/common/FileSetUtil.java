/*
 * Copyright © 2015, 2016 Cask Data, Inc.
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
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for configuring file sets during pipeline configuration.
 * TODO (CDAP-6211): Why do we lower-case the schema for Parquet but not for Avro?
 */
public class FileSetUtil {

  private static final Logger LOG = LoggerFactory.getLogger(FileSetUtil.class);
  private static final String AVRO_OUTPUT_CODEC = "avro.output.codec";
  private static final String MAPRED_OUTPUT_COMPRESS = "mapred.output.compress";
  private static final String AVRO_SCHEMA_OUTPUT_KEY = "avro.schema.output.key";
  private static final String CODEC_SNAPPY = "snappy";
  private static final String CODEC_DEFLATE = "deflate";
  private static final String CODEC_GZIP = "gzip";
  private static final String CODEC_LZO = "lzo";
  private static final String PARQUET_AVRO_SCHEMA = "parquet.avro.schema";
  private static final String PARQUET_COMPRESSION = "parquet.compression";

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
    hConf.clear();
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
   * Configure a file set to use ORC file format with a given schema. The schema is parsed
   * validated and converted into a Hive schema which is compatible with ORC format. The file set is configured to use
   * ORC input and output format, and also configured for Explore to use Hive. The schema is added
   * to the file set properties in all the different required ways:
   * <ul>
   *   <li>As a top-level dataset property;</li>
   *   <li>As the schema for the input and output format;</li>
   *   <li>As the schema to be used by the ORC serde (which is used by Hive).</li>
   * </ul>
   *
   * @param configuredSchema the original schema configured for the table
   * @param properties a builder for the file set properties
   */
  public static void configureORCFileSet(String configuredSchema, FileSetProperties.Builder properties)  {
    //TODO test if complex cases run with lowercase schema only
    String lowerCaseSchema = configuredSchema.toLowerCase();
    String hiveSchema = parseHiveSchema(lowerCaseSchema, configuredSchema);
    hiveSchema = hiveSchema.substring(1, hiveSchema.length() - 1);

    String orcSchema = parseOrcSchema(configuredSchema);

    properties.setInputFormat(OrcInputFormat.class)
      .setOutputFormat(OrcOutputFormat.class)
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
      .setSerDe("org.apache.hadoop.hive.ql.io.orc.OrcSerde")
      .setExploreSchema(hiveSchema)
      .setEnableExploreOnCreate(true)
      .setInputProperty("orc.mapred.output.schema", orcSchema)
      .setOutputProperty("orc.mapred.output.schema", orcSchema)
      .add(DatasetProperties.SCHEMA, configuredSchema)
      .build();
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
    // validate and parse schema as Avro
    Schema avroSchema = parseAvroSchema(configuredSchema, configuredSchema);

    properties
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", configuredSchema)
      .add(DatasetProperties.SCHEMA, configuredSchema);

    Job job = createJobForConfiguration();
    Configuration hConf = job.getConfiguration();
    hConf.clear();
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

  private static String parseOrcSchema(String configuredSchema) {
    try {
      co.cask.cdap.api.data.schema.Schema schemaObj = co.cask.cdap.api.data.schema.Schema.parseJson(configuredSchema);
      StringBuilder builder = new StringBuilder();
      HiveSchemaConverter.appendType(builder, schemaObj);
      return builder.toString();
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("%s is not a valid schema", configuredSchema), e);
    } catch (UnsupportedTypeException e) {
      throw new IllegalArgumentException(String.format("Could not create hive schema from %s", configuredSchema), e);
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

  /**
   * Sets the compression options for an Avro file set format. Also, sets the schema output key to the schema provided.
   * The map-reduce output compression is set to true, and the compression codec can be set to one of
   * the following:
   * <ul>
   *   <li>snappy</li>
   *   <li>deflate</li>
   * </ul>
   * @param compressionCodec compression code provided, can be either snappy or deflate
   * @param schema output schema to be set as the schema output key for the file set
   * @param isOutputProperty boolean value to identify if the compression options are used as output property for
   *                         FilesetProperties.Builder
   * @return map of string to be set as configuration or output properties in FileSetProperties.Builder
   */
  public static Map<String, String> getAvroCompressionConfiguration(String compressionCodec, String schema,
                                                                    Boolean isOutputProperty) {
    Map<String, String> conf = new HashMap<>();
    String prefix = "";
    if (isOutputProperty) {
      prefix = FileSetProperties.OUTPUT_PROPERTIES_PREFIX;
    }
    conf.put(prefix + AVRO_SCHEMA_OUTPUT_KEY, schema);
    if (compressionCodec != null && !compressionCodec.equalsIgnoreCase("None")) {
      conf.put(prefix + MAPRED_OUTPUT_COMPRESS, "true");
      switch (compressionCodec.toLowerCase()) {
        case CODEC_SNAPPY:
          conf.put(prefix + AVRO_OUTPUT_CODEC, CODEC_SNAPPY);
          break;
        case CODEC_DEFLATE:
          conf.put(prefix + AVRO_OUTPUT_CODEC, CODEC_DEFLATE);
          break;
        default:
          throw new IllegalArgumentException("Unsupported compression codec " + compressionCodec);
      }
    }
    return conf;
  }

  /**
   * Sets the compression options for an Avro file set format. Also, sets the schema output key to the schema provided.
   * The compression codec can be set to one of the following:
   * <ul>
   *   <li>SNAPPY</li>
   *   <li>GZIP</li>
   *   <li>LZO</li>
   * </ul>
   * @param compressionCodec compression code selected by user. Can be either snappy or deflate
   * @param schema output schema to be set as the schema output key for the file set
   * @param isOutputProperty boolean value to identify if the compression options are as output property for
   *                         FilesetProperties Builder
   * @return map of string to be set as configuration or output properties in FileSetProperties.Builder
   */
  public static Map<String, String> getParquetCompressionConfiguration(String compressionCodec, String schema,
                                                                       Boolean isOutputProperty) {
    Map<String, String> conf = new HashMap<>();
    String prefix = "";
    if (isOutputProperty) {
      prefix = FileSetProperties.OUTPUT_PROPERTIES_PREFIX;
    }
    conf.put(prefix + PARQUET_AVRO_SCHEMA, schema);
    if (compressionCodec != null && !compressionCodec.equalsIgnoreCase("None")) {
      switch (compressionCodec.toLowerCase()) {
        case CODEC_SNAPPY:
          conf.put(prefix + PARQUET_COMPRESSION, CODEC_SNAPPY.toUpperCase());
          break;
        case CODEC_GZIP:
          conf.put(prefix + PARQUET_COMPRESSION, CODEC_GZIP.toUpperCase());
          break;
        case CODEC_LZO:
          conf.put(prefix + PARQUET_COMPRESSION, CODEC_LZO.toUpperCase());
          break;
        default:
          throw new IllegalArgumentException("Unsupported compression codec " + compressionCodec);
      }
    }
    return conf;
  }
}
