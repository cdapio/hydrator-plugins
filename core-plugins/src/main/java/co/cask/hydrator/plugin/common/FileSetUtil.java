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
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

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
   * @param properties a builder for the file set properties
   * @param configuredSchema the original schema configured for the table
   */
  public static void configureAvroFileSet(FileSetProperties.Builder properties, @Nullable String configuredSchema) {
    // validate and parse schema as Avro, and attempt to convert it into a Hive schema
    Schema avroSchema = configuredSchema == null ? null : parseAvroSchema(configuredSchema, configuredSchema);
    String hiveSchema = (configuredSchema == null || avroSchema.getType() != Schema.Type.RECORD)
      ? null
      : parseHiveSchema(configuredSchema, configuredSchema);

    properties
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class);

    if (configuredSchema != null) {
      properties
        .setTableProperty("avro.schema.literal", configuredSchema)
        .add(DatasetProperties.SCHEMA, configuredSchema);
    }

    if (hiveSchema != null) {
      properties
        .setEnableExploreOnCreate(true)
        .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
        .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
        .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
        .setExploreSchema(hiveSchema.substring(1, hiveSchema.length() - 1));
    } else {
      properties.setEnableExploreOnCreate(false);
    }

    if (avroSchema != null) {
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
    co.cask.cdap.api.data.schema.Schema schemaObj = null;
    try {
      schemaObj = co.cask.cdap.api.data.schema.Schema.parseJson(configuredSchema);
      StringBuilder builder = new StringBuilder();
      HiveSchemaConverter.appendType(builder, schemaObj);
      return builder.toString();
    } catch (IOException e) {
      LOG.debug("{} is not a valid schema", configuredSchema, e);
      throw new IllegalArgumentException(String.format("{} is not a valid schema", configuredSchema), e);
    } catch (UnsupportedTypeException e) {
      throw new IllegalArgumentException(String.format("Could not create hive schema from {}", configuredSchema), e);
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
  public static Map<String, String> getAvroCompressionConfiguration(String compressionCodec, @Nullable String schema,
                                                                    boolean isOutputProperty) {
    Map<String, String> conf = new HashMap<>();
    String prefix = "";
    if (isOutputProperty) {
      prefix = FileSetProperties.OUTPUT_PROPERTIES_PREFIX;
    }
    if (schema != null) {
      conf.put(prefix + AVRO_SCHEMA_OUTPUT_KEY, schema);
    }
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
                                                                       boolean isOutputProperty) {
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

  private static final Set<String> RESERVED_KWD =
    new HashSet<String>(Arrays.asList("ALL", "ALTER", "AND", "ARRAY", "AS", "AUTHORIZATION",
                                      "BETWEEN", "BIGINT", "BINARY", "BOOLEAN", "BOTH", "BY", "CASE", "CAST", "CHAR",
                                      "COLUMN", "CONF", "CREATE", "CROSS", "CUBE", "CURRENT", "CURRENT_DATE",
                                      "CURRENT_TIMESTAMP", "CURSOR", "DATABASE", "DATE", "DECIMAL", "DELETE",
                                      "DESCRIBE", "DISTINCT", "DOUBLE", "DROP", "ELSE", "END", "EXCHANGE", "EXISTS",
                                      "EXTENDED", "EXTERNAL", "FALSE", "FETCH", "FLOAT", "FOLLOWING", "FOR", "FROM",
                                      "FULL", "FUNCTION", "GRANT", "GROUP", "GROUPING", "HAVING", "IF", "IMPORT", "IN",
                                      "INNER", "INSERT", "INT", "INTERSECT", "INTERVAL", "INTO", "IS", "JOIN",
                                      "LATERAL", "LEFT", "LESS", "LIKE", "LOCAL", "MACRO", "MAP", "MORE", "NONE",
                                      "NOT", "NULL", "OF", "ON", "OR", "ORDER", "OUT", "OUTER", "OVER", "PARTIALSCAN",
                                      "PARTITION", "PERCENT", "PRECEDING", "PRESERVE", "PROCEDURE", "RANGE", "READS",
                                      "REDUCE", "REVOKE", "RIGHT", "ROLLUP", "ROW", "ROWS", "SELECT", "SET", "SMALLINT",
                                      "TABLE", "TABLESAMPLE", "THEN", "TIMESTAMP", "TO", "TRANSFORM", "TRIGGER", "TRUE",
                                      "TRUNCATE", "UNBOUNDED", "UNION", "UNIQUEJOIN", "UPDATE", "USER", "USING",
                                      "UTC_TMESTAMP", "VALUES", "VARCHAR", "WHEN", "WHERE", "WINDOW", "WITH"));

  private static String translateName(String field) {
    if (RESERVED_KWD.contains(field.toUpperCase())) {
      return String.format("`%s`", field);
    }
    return field;
  }

  /**
   *
   */
  public static final class SchemaTypeAdapter extends TypeAdapter<co.cask.cdap.api.data.schema.Schema> {

    private static final String TYPE = "type";
    private static final String NAME = "name";
    private static final String SYMBOLS = "symbols";
    private static final String ITEMS = "items";
    private static final String KEYS = "keys";
    private static final String VALUES = "values";
    private static final String FIELDS = "fields";

    @Override
    public void write(JsonWriter writer, co.cask.cdap.api.data.schema.Schema schema) throws IOException {
      if (schema == null) {
        writer.nullValue();
        return;
      }
      Set<String> knownRecords = new HashSet<>();
      write(writer, schema, knownRecords);
    }

    @Override
    public co.cask.cdap.api.data.schema.Schema read(JsonReader reader) throws IOException {
      return read(reader, new HashMap<String, co.cask.cdap.api.data.schema.Schema>());
    }

    /**
     * Reads json value and convert it into {@link co.cask.cdap.api.data.schema.Schema} object.
     *
     * @param reader Source of json
     * @param knownRecords Set of record name already encountered during the reading.
     * @return A {@link co.cask.cdap.api.data.schema.Schema} reflecting the json.
     * @throws IOException Any error during reading.
     */
    private co.cask.cdap.api.data.schema.Schema read(JsonReader reader, Map<String, co.cask.cdap.api.data.schema.Schema> knownRecords) throws IOException {
      JsonToken token = reader.peek();
      switch (token) {
        case NULL:
          return null;
        case STRING: {
          // Simple type or know record type
          String name = reader.nextString();
          if (knownRecords.containsKey(name)) {
            co.cask.cdap.api.data.schema.Schema schema = knownRecords.get(name);
          /*
             schema is null and in the map if this is a recursive reference. For example,
             if we're looking at the inner 'node' record in the example below:
             {
               "type": "record",
               "name": "node",
               "fields": [{
                 "name": "children",
                 "type": [{
                   "type": "array",
                   "items": ["node", "null"]
                 }, "null"]
               }, {
                 "name": "data",
                 "type": "int"
               }]
             }
           */
            return schema == null ? co.cask.cdap.api.data.schema.Schema.recordOf(name) : schema;
          }
          return co.cask.cdap.api.data.schema.Schema.of(co.cask.cdap.api.data.schema.Schema.Type.valueOf(name.toUpperCase()));
        }
        case BEGIN_ARRAY:
          // Union type
          return readUnion(reader, knownRecords);
        case BEGIN_OBJECT:
          return readObject(reader, knownRecords);
      }
      throw new IOException("Malformed schema input.");
    }

    /**
     * Read JSON object and return Schema corresponding to it.
     * @param reader JsonReader used to read the json object
     * @param knownRecords Set of record name already encountered during the reading.
     * @return Schema reflecting json
     * @throws IOException when error occurs during reading json
     */
    private co.cask.cdap.api.data.schema.Schema readObject(JsonReader reader, Map<String, co.cask.cdap.api.data.schema.Schema> knownRecords) throws IOException {
      reader.beginObject();
      // Type of the schema
      co.cask.cdap.api.data.schema.Schema.Type schemaType = null;
      // Name of the element
      String elementName = null;
      // Store enum values for ENUM type
      List<String> enumValues = new ArrayList<>();
      // Store schema for key and value for MAP type
      co.cask.cdap.api.data.schema.Schema keys = null;
      co.cask.cdap.api.data.schema.Schema values = null;
      // List of fields for RECORD type
      List<co.cask.cdap.api.data.schema.Schema.Field> fields = null;
      // List of items for ARRAY type
      co.cask.cdap.api.data.schema.Schema items = null;
      // Loop through current object and populate the fields as required
      // For ENUM type List of enumValues will be populated
      // For ARRAY type items will be populated
      // For MAP type keys and values will be populated
      // For RECORD type fields will be popuated
      while (reader.hasNext()) {
        String name = reader.nextName();
        switch (name) {
          case TYPE:
            schemaType = co.cask.cdap.api.data.schema.Schema.Type.valueOf(reader.nextString().toUpperCase());
            break;
          case NAME:
            elementName = reader.nextString();
            if (schemaType == co.cask.cdap.api.data.schema.Schema.Type.RECORD) {
            /*
              Put a null schema in the map for the recursive references.
              For example, if we are looking at the outer 'node' reference in the example below, we
              add the record name in the knownRecords map, so that when we get to the inner 'node'
              reference, we know that its a record type and not a Schema.Type.
              {
                "type": "record",
                "name": "node",
                "fields": [{
                  "name": "children",
                  "type": [{
                    "type": "array",
                    "items": ["node", "null"]
                  }, "null"]
                },
                {
                  "name": "data",
                  "type": "int"
                }]
              }
              Full schema corresponding to this RECORD will be put in knownRecords once the fields in the
              RECORD are explored.
            */
              knownRecords.put(elementName, null);
            }
            break;
          case SYMBOLS:
            enumValues = readEnum(reader);
            break;
          case ITEMS:
            items = read(reader, knownRecords);
            break;
          case KEYS:
            keys = read(reader, knownRecords);
            break;
          case VALUES:
            values = read(reader, knownRecords);
            break;
          case FIELDS:
            fields = getFields(name, reader, knownRecords);
            knownRecords.put(elementName, co.cask.cdap.api.data.schema.Schema.recordOf(elementName, fields));
            break;
          default:
            reader.skipValue();
            break;
        }
      }
      reader.endObject();
      if (schemaType == null) {
        throw new IllegalStateException("Schema type cannot be null.");
      }
      co.cask.cdap.api.data.schema.Schema schema;
      switch (schemaType) {
        case ARRAY:
          schema = co.cask.cdap.api.data.schema.Schema.arrayOf(items);
          break;
        case ENUM:
          schema = co.cask.cdap.api.data.schema.Schema.enumWith(enumValues);
          break;
        case MAP:
          schema = co.cask.cdap.api.data.schema.Schema.mapOf(keys, values);
          break;
        case RECORD:
          schema = co.cask.cdap.api.data.schema.Schema.recordOf(elementName, fields);
          break;
        default:
          schema = co.cask.cdap.api.data.schema.Schema.of(schemaType);
          break;
      }
      return schema;
    }

    /**
     * Constructs {@link co.cask.cdap.api.data.schema.Schema.Type#UNION UNION} type schema from the json input.
     *
     * @param reader The {@link JsonReader} for streaming json input tokens.
     * @param knownRecords Map of record names and associated schema already encountered during the reading
     * @return A {@link co.cask.cdap.api.data.schema.Schema} of type {@link co.cask.cdap.api.data.schema.Schema.Type#UNION UNION}.
     * @throws IOException When fails to construct a valid schema from the input.
     */
    private co.cask.cdap.api.data.schema.Schema readUnion(JsonReader reader, Map<String, co.cask.cdap.api.data.schema.Schema> knownRecords) throws IOException {
      List<co.cask.cdap.api.data.schema.Schema> unionSchemas = new ArrayList<>();
      reader.beginArray();
      while (reader.peek() != JsonToken.END_ARRAY) {
        unionSchemas.add(read(reader, knownRecords));
      }
      reader.endArray();
      return co.cask.cdap.api.data.schema.Schema.unionOf(unionSchemas);
    }

    /**
     * Returns the {@link List} of enum values from the json input.
     * @param reader The {@link JsonReader} for streaming json input tokens.
     * @return a list of enum values
     * @throws IOException When fails to parse the input json.
     */
    private List<String> readEnum(JsonReader reader) throws IOException {
      List<String> enumValues = new ArrayList<>();
      reader.beginArray();
      while (reader.peek() != JsonToken.END_ARRAY) {
        enumValues.add(reader.nextString());
      }
      reader.endArray();
      return enumValues;
    }

    /**
     * Get the list of {@link co.cask.cdap.api.data.schema.Schema.Field} associated with current RECORD.
     * @param recordName the name of the RECORD for which fields to be returned
     * @param reader the reader to read the record
     * @param knownRecords record names already encountered during the reading
     * @return the list of fields associated with the current record
     * @throws IOException when error occurs during reading the json
     */
    private List<co.cask.cdap.api.data.schema.Schema.Field> getFields(String recordName, JsonReader reader, Map<String, co.cask.cdap.api.data.schema.Schema> knownRecords)
      throws IOException {
      knownRecords.put(recordName, null);
      List<co.cask.cdap.api.data.schema.Schema.Field> fieldBuilder = new ArrayList<>();
      reader.beginArray();
      while (reader.peek() != JsonToken.END_ARRAY) {
        reader.beginObject();
        String fieldName = null;
        co.cask.cdap.api.data.schema.Schema innerSchema = null;

        while (reader.hasNext()) {
          String name = reader.nextName();
          switch(name) {
            case NAME:
              fieldName = reader.nextString();
              break;
            case TYPE:
              innerSchema = read(reader, knownRecords);
              break;
            default:
              reader.skipValue();
          }
        }
        fieldBuilder.add(co.cask.cdap.api.data.schema.Schema.Field.of(fieldName, innerSchema));
        reader.endObject();
      }
      reader.endArray();
      return fieldBuilder;
    }

    /**
     * Writes the given {@link co.cask.cdap.api.data.schema.Schema} into json.
     *
     * @param writer A {@link JsonWriter} for emitting json.
     * @param schema The {@link co.cask.cdap.api.data.schema.Schema} object to encode to json.
     * @param knownRecords Set of record names that has already been encoded.
     * @return The same {@link JsonWriter} as the one passed in.
     * @throws IOException When fails to encode the schema into json.
     */
    private JsonWriter write(JsonWriter writer, co.cask.cdap.api.data.schema.Schema schema, Set<String> knownRecords) throws IOException {
      // Simple type, just emit the type name as a string
      if (schema.getType().isSimpleType()) {
        return writer.value(schema.getType().name().toLowerCase());
      }

      // Union type is an array of schemas
      if (schema.getType() == co.cask.cdap.api.data.schema.Schema.Type.UNION) {
        writer.beginArray();
        for (co.cask.cdap.api.data.schema.Schema unionSchema : schema.getUnionSchemas()) {
          write(writer, unionSchema, knownRecords);
        }
        return writer.endArray();
      }

      // If it is a record that refers to a previously defined record, just emit the name of it
      if (schema.getType() == co.cask.cdap.api.data.schema.Schema.Type.RECORD && knownRecords.contains(schema.getRecordName())) {
        return writer.value(translateName(schema.getRecordName()));
      }
      // Complex types, represented as an object with "type" property carrying the type name
      writer.beginObject().name(TYPE).value(schema.getType().name().toLowerCase());
      switch (schema.getType()) {
        case ENUM:
          // Emits all enum values as an array, keyed by "symbols"
          writer.name(SYMBOLS).beginArray();
          for (String enumValue : schema.getEnumValues()) {
            writer.value(enumValue);
          }
          writer.endArray();
          break;

        case ARRAY:
          // Emits the schema of the array component type, keyed by "items"
          write(writer.name(ITEMS), schema.getComponentSchema(), knownRecords);
          break;

        case MAP:
          // Emits schema of both key and value types, keyed by "keys" and "values" respectively
          Map.Entry<co.cask.cdap.api.data.schema.Schema, co.cask.cdap.api.data.schema.Schema> mapSchema = schema.getMapSchema();
          write(writer.name(KEYS), mapSchema.getKey(), knownRecords);
          write(writer.name(VALUES), mapSchema.getValue(), knownRecords);
          break;

        case RECORD:
          // Emits the name of record, keyed by "name"
          knownRecords.add(schema.getRecordName());
          writer.name(NAME).value(translateName(schema.getRecordName()))
            .name(FIELDS).beginArray();
          // Each field is an object, with field name keyed by "name" and field schema keyed by "type"
          for (co.cask.cdap.api.data.schema.Schema.Field field : schema.getFields()) {
            writer.beginObject().name(NAME).value(translateName(field.getName()));
            write(writer.name(TYPE), field.getSchema(), knownRecords);
            writer.endObject();
          }
          writer.endArray();
          break;
      }
      writer.endObject();

      return writer;
    }
  }


}
