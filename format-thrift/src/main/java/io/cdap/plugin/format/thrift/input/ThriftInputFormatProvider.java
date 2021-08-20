/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.plugin.format.thrift.input;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.format.input.PathTrackingConfig;
import io.cdap.plugin.format.input.PathTrackingInputFormatProvider;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Input reading logic for Thrift files.
 */
@Plugin(type = ValidatingInputFormat.PLUGIN_TYPE)
@Name(ThriftInputFormatProvider.NAME)
@Description(ThriftInputFormatProvider.DESC)
public class ThriftInputFormatProvider extends PathTrackingInputFormatProvider<ThriftInputFormatProvider.ThriftConfig> {
    static final String NAME = "thrift";
    static final String DESC = "Plugin for reading files in thrif format.";
    public static final PluginClass PLUGIN_CLASS =
            new PluginClass(ValidatingInputFormat.PLUGIN_TYPE, NAME, DESC, ThriftInputFormatProvider.class.getName(),
                    "conf", PathTrackingConfig.FIELDS);
    // this has to be here to be able to instantiate the correct plugin property field
    private final ThriftConfig conf;

    public ThriftInputFormatProvider(ThriftConfig conf) {
        super(conf);
        this.conf = conf;
    }

    @Override
    public String getInputFormatClassName() {
        return ThriftTextInputFormat.class.getName();
    }

    @Override
    protected void addFormatProperties(Map<String, String> properties) {
        Schema schema = conf.getSchema();
        if (schema != null) {
//        TODO: Are there any specific properties required to process Thrift
//            e.g for AVRO `properties.put("avro.schema.input.key", schema.toString());`
        }
    }

    //TODO: Once we understand our Schema, write some validations
    // e.g make sure Required Field exists and has the right schema name ?
    // e.g. for Text files, Schema requires a field called body exists (this is the text itself per record)
    @Override
    public void validate(FormatContext context) {
        if (conf.containsMacro(ThriftConfig.NAME_SCHEMA)) {
            return;
        }

        FailureCollector collector = context.getFailureCollector();
        Schema schema;
        try {
            schema = conf.getSchema();
        } catch (Exception e) {
            collector.addFailure(e.getMessage(), null).withConfigProperty(ThriftConfig.NAME_SCHEMA)
                    .withStacktrace(e.getStackTrace());
            throw collector.getOrThrowException();
        }

        String pathField = conf.getPathField();
        //TODO: Figure out any Required fields // text must contain '<INSERT FIELD REQUIRED FIELD>' as type '<REQUIRED TYPE>'.

        Schema.Field requiredFieldA = schema.getField(ThriftConfig.REQUIRED_FIELD_NAME_A);
        if (requiredFieldA == null) {
            //TODO Are there any fields a THRIFT format must have ?
            collector.addFailure("The schema for the 'thrift' format must have a field named 'REQUIRED_FIELD_NAME_A'.", null)
                    .withConfigProperty(ThriftConfig.NAME_SCHEMA);
        } else {
            /*
                Here we do:
                    * Get the Required Field
                    * Assert it is of the right Schema type
             */

            Schema requiredFieldASchema = requiredFieldA.getSchema();
            requiredFieldASchema = requiredFieldASchema.isNullable() ? requiredFieldASchema.getNonNullable() : requiredFieldASchema;
            Schema.Type requiredFieldASchemaType = requiredFieldASchema.getType();
            if (requiredFieldASchemaType != Schema.Type.STRING) { //TODO What is the required field a type of ?
                collector.addFailure(
                        String.format("The 'body' field is of unexpected type '%s'.'", requiredFieldASchema.getDisplayName()),
                        "Change type to '<REQUIRED TYPE>'.").withOutputSchemaField(ThriftConfig.REQUIRED_FIELD_NAME_A);
            }
        }

        // fields should be body (required), offset (optional), [pathfield] (optional)
        boolean expectOptionalA = schema.getField(ThriftConfig.OPTIONAL_FIELD_NAME_A) != null;
        boolean expectOptionalB =schema.getField(ThriftConfig.OPTIONAL_FIELD_NAME_B) != null;
        int numExpectedFields = 1;
        if (expectOptionalA) {
            numExpectedFields++;
        }
        if (expectOptionalB) {
            numExpectedFields++;
        }

        int numFields = schema.getFields().size();
        if (numFields > numExpectedFields) {
            for (Schema.Field field : schema.getFields()) {
                String expectedFields;
                if (expectOptionalA && expectOptionalB) {
                    expectedFields = String.format("'offset', 'body', and '%s' fields", pathField);
                } else if (expectOptionalA) {
                    expectedFields = "'offset' and 'body' fields";
                } else if (expectOptionalB) {
                    expectedFields = String.format("'body' and '%s' fields", pathField);
                } else {
                    expectedFields = "'body' field";
                }

                if (field.getName().equals(ThriftConfig.REQUIRED_FIELD_NAME_A) || (expectOptionalB && field.getName().equals(pathField))
                        || field.getName().equals(ThriftConfig.OPTIONAL_FIELD_NAME_A)) {
                    continue;
                }

                collector.addFailure(
                        String.format("The schema for the 'text' format must only contain the '%s'.", expectedFields),
                        String.format("Remove additional field '%s'.", field.getName())).withOutputSchemaField(field.getName());
            }
        }
    }

    public static Schema getDefaultSchema(@Nullable String pathField) {
        //TODO Should we have some sort of a Default Schema for Thrift ?? e.g. PARC ?
        // See other InputFormatProviers classes
        // e.g. for Text you have
        // * Offset/ Body (required) / path
        return null;
    }

    public static class ThriftConfig extends PathTrackingConfig {

        public static final Map<String, PluginPropertyField> THRIFT_FIELDS;
        public static final String REQUIRED_FIELD_NAME_A = "BOB_A";
        public static final String OPTIONAL_FIELD_NAME_A = "ROGER_A";
        public static final String OPTIONAL_FIELD_NAME_B = "ROGER_B";

        static {
            //TODO we are using the default path tracking config fields - do we need a specific thrift one ?
            Map<String, PluginPropertyField> fields = new HashMap<>(FIELDS);

            //TODO Put some fields ?
//            fields.put("skipHeader", new PluginPropertyField("skipHeader", SKIP_HEADER_DESC,
//                    "boolean", false, true));
            THRIFT_FIELDS = Collections.unmodifiableMap(fields);
        }

        /**
         * Return the configured schema, or the default schema if none was given. Should never be called if the
         * schema contains a macro
         */
        @Override
        public Schema getSchema() {
            if (containsMacro(NAME_SCHEMA)) {
                return null;
            }
            if (Strings.isNullOrEmpty(schema)) {
                return getDefaultSchema(pathField);
            }
            try {
                return Schema.parseJson(schema);
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid schema: " + e.getMessage(), e);
            }
        }

    }
}
