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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.LookupConfig;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transforms records using custom mapping provided by the config.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("ValueMapper")
@Description("Maps and converts record values using a mapping dataset")
public class ValueMapper extends Transform<StructuredRecord, StructuredRecord> {

  private static final Gson GSON = new Gson();
  private final Config config;
  private final Map<Schema, Schema> schemaCache = Maps.newHashMap();
  private final Map<String, String> fieldsMapping = Maps.newHashMap();
  private final Map<String, ValueMapperLookUp> datasetMapping = new HashMap<>();

  // for unit tests, otherwise config is injected by plugin framework.
  public ValueMapper(Config config) {
    this.config = config;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    Schema outputSchema = getOutputSchema(input.getSchema());

    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    for (Schema.Field sourceField : input.getSchema().getFields()) {

      if (!input.get(sourceField.getName()).equals("NULL") &&
              !input.get(sourceField.getName()).equals("EMPTY") &&
              fieldsMapping.containsKey(sourceField.getName())) {

        if (datasetMapping.containsKey(sourceField.getName())) {
          ValueMapperLookUp valueMapperLookUp = datasetMapping.get(sourceField.getName());

          if (valueMapperLookUp.lookup(input.get(sourceField.getName()).toString()) != null &&
                  valueMapperLookUp.lookup(input.get(sourceField.getName()).toString()).toString().trim().length() > 0) {

            builder.set(fieldsMapping.get(sourceField.getName()),
                    valueMapperLookUp.lookup(input.get(sourceField.getName()).toString()));
          } else {
            builder.set(fieldsMapping.get(sourceField.getName()), "Empty");
          }
        } else {
          // for those source field whose mapping is not present
          builder.set(fieldsMapping.get(sourceField.getName()), "Empty");
        }

      } else {
        // for those source field whose values are either NULL or EMPTY
        builder.set(sourceField.getName(), input.get(sourceField.getName()
        ).toString());
      }
    }

    emitter.emit(builder.build());
  }

  /**
   * Creates output schema record using source and destination field mapping provided by user.
   */
  private Schema getOutputSchema(Schema inputSchema) throws IllegalArgumentException {
    Schema outputSchema = schemaCache.get(inputSchema);
    if (outputSchema != null) {
      return outputSchema;
    }
    List<Schema.Field> outputFields = Lists.newArrayList();
    for (Schema.Field inputField : inputSchema.getFields()) {
      if (inputField.getSchema().getType() != Schema.Type.STRING) {
        throw new IllegalArgumentException("Input field "
                + inputField.getName() + " type should be String");
      } else {
        if (fieldsMapping.containsKey(inputField.getName())) {
          outputFields.add(Schema.Field.of(fieldsMapping.get(inputField.getName()),
                  Schema.of(Schema.Type.STRING)));
        } else {
          outputFields.add(inputField);
        }
      }
    }
    outputSchema = Schema.recordOf(inputSchema.getRecordName() + ".formatted", outputFields);
    schemaCache.put(inputSchema, outputSchema);
    return outputSchema;
  }

  /**
   * Parse configuration provided by user
   */
  public void parseConfiguration(String mapping, TransformContext context) {

    JsonParser jsonParser = new JsonParser();
    JsonElement jsonMapping = jsonParser.parse(mapping);
    JsonArray mappingArray = jsonMapping.getAsJsonObject().get("mapping").getAsJsonArray();
    for (JsonElement mappingElement : mappingArray) {
      String sourceField = mappingElement.getAsJsonObject().get("sourceField").getAsString();
      String targetField = mappingElement.getAsJsonObject().get("targetField").getAsString();
      fieldsMapping.put(sourceField, targetField);
      ValueMapperLookUp lookUpTable = init(context, mappingElement.getAsJsonObject().get("lookup").toString());
      datasetMapping.put(sourceField, lookUpTable);
    }
  }

  /**
   * Creates a look up table
   */
  private ValueMapperLookUp init(LookupProvider context, String lookUp) throws IllegalArgumentException {

    LookupConfig lookUpConfig;
    try {
      lookUpConfig = GSON.fromJson(lookUp, LookupConfig.class);
    } catch (JsonSyntaxException e) {
      throw new IllegalArgumentException("Invalid lookup config. Expected map of string to string", e);
    }

    ValueMapperLookupProvider valueMapperLookupProvider = new ValueMapperLookupProvider(context, lookUpConfig);
    String lookUpTable = parseLookUpTableName(lookUp);

    ValueMapperLookUp lookupTable = valueMapperLookupProvider.provide(lookUpTable);
    return lookupTable;
  }

  /**
   * parse lookup table name from lookup object
   */
  private String parseLookUpTableName(String lookUp) {
    String tableName = null;
    JsonParser jsonParser = new JsonParser();
    JsonElement jsonMapping = jsonParser.parse(lookUp);
    JsonElement table = jsonMapping.getAsJsonObject().get("tables");
    for (Map.Entry entry : table.getAsJsonObject().entrySet()) {
      tableName = entry.getKey().toString();
      break;
    }
    return tableName;
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    parseConfiguration(config.mapping, context);
  }

  /**
   * Configuration for the ValueMapper transform.
   */
  public static class Config extends PluginConfig {

    //TODO input parameters may be changed after having three different input params from UI
    @Name("mapping")
    @Description("Specify the source and target field mapping and lookup dataset name.")
    private final String mapping;

    public Config(String mapping) {
      this.mapping = mapping;
    }
  }
}
