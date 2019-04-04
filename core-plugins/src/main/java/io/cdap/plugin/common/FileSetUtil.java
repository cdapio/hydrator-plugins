/*
 * Copyright © 2015, 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin.common;

import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;

/**
 * Utilities for configuring file sets during pipeline configuration.
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
    String hiveSchema = parseHiveSchema(configuredSchema, configuredSchema);

    properties
      .setEnableExploreOnCreate(true)
      .setExploreFormat("parquet")
      .setExploreSchema(hiveSchema.substring(1, hiveSchema.length() - 1))
      .add(DatasetProperties.SCHEMA, configuredSchema);
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

    properties.setExploreInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
      .setSerDe("org.apache.hadoop.hive.ql.io.orc.OrcSerde")
      .setExploreSchema(hiveSchema)
      .setEnableExploreOnCreate(true)
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
    properties
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", configuredSchema)
      .add(DatasetProperties.SCHEMA, configuredSchema);
  }

  /*----- private helpers ----*/

  private static String parseHiveSchema(String schemaString, String configuredSchema) {
    try {
      return HiveSchemaConverter.toHiveSchema(io.cdap.cdap.api.data.schema.Schema.parseJson(schemaString));
    } catch (UnsupportedTypeException e) {
      throw new IllegalArgumentException("Schema " + configuredSchema + " is not supported as a Hive schema.", e);
    } catch (Exception e) {
      throw new IllegalArgumentException("Schema " + configuredSchema + " is invalid.", e);
    }
  }
}
