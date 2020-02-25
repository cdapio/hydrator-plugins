/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.Field;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.plugin.common.FieldEncryptor;
import io.cdap.plugin.common.KeystoreConf;
import io.cdap.plugin.common.TransformLineageRecorderUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.crypto.Cipher;

/**
 * Decrypts record fields.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Decryptor")
@Description("Decrypts fields of records.")
public final class Decryptor extends Transform<StructuredRecord, StructuredRecord> {
  private final Conf conf;
  private Set<String> decryptFields;
  private Schema schema;
  private FieldEncryptor fieldEncryptor;

  public Decryptor(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer configurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = configurer.getFailureCollector();
    Schema schema = conf.getSchema(collector);
    Schema inputSchema = configurer.getInputSchema();

    validateDecryptFields(collector, inputSchema);
    configurer.setOutputSchema(schema);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    schema = conf.getSchema(collector);
    collector.getOrThrowException();

    decryptFields = conf.getDecryptFields();
    fieldEncryptor = new FileBasedFieldEncryptor(conf, Cipher.DECRYPT_MODE);
    fieldEncryptor.initialize();
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    decryptFields = conf.getDecryptFields();
    if (context.getInputSchema() == null || context.getInputSchema().getFields() == null) {
      return;
    }

    // Use all decryptFields from conf that also exist in input schema.
    List<String> decryptedFields = TransformLineageRecorderUtils.getFields(context.getInputSchema()).stream()
      .filter(decryptFields::contains)
      .collect(Collectors.toList());

    List<String> identityFields = TransformLineageRecorderUtils.getFields(context.getInputSchema());
    identityFields.removeAll(decryptedFields);

    List<FieldOperation> output = new ArrayList<>();
    output.addAll(TransformLineageRecorderUtils.generateOneToOnes(decryptedFields, "decrypt",
      "Decrypted the requested fields."));
    output.addAll(TransformLineageRecorderUtils.generateOneToOnes(identityFields, "identity",
      TransformLineageRecorderUtils.IDENTITY_TRANSFORM_DESCRIPTION));
    context.record(output);
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    for (Field field : in.getSchema().getFields()) {
      if (decryptFields.contains(field.getName())) {
        Schema fieldSchema = field.getSchema();
        Schema targetSchema = schema.getField(field.getName()).getSchema();
        Object val = in.get(field.getName());
        if (fieldSchema.isNullable() && val == null) {
          recordBuilder.set(field.getName(), null);
        } else {
          Schema.Type fieldType =
            fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
          if (fieldType != Schema.Type.BYTES) {
            throw new IllegalArgumentException(String.format(
              "Cannot decrypt field '%s' because it is of type '%s' instead of bytes.",
              field.getName(), fieldType));
          }
          recordBuilder.set(field.getName(), fieldEncryptor.decrypt((byte[]) val, targetSchema));
        }
      } else {
        recordBuilder.set(field.getName(), in.get(field.getName()));
      }
    }
    emitter.emit(recordBuilder.build());
  }

  private void validateDecryptFields(FailureCollector collector, @Nullable Schema inputSchema) {
    if (inputSchema == null) {
      return;
    }

    Set<String> decryptFields = conf.getDecryptFields();
    for (Field inField : inputSchema.getFields()) {
      if (!decryptFields.contains(inField.getName())) {
        continue;
      }
      Schema nonNullableSchema = inField.getSchema().isNullable() ?
        inField.getSchema().getNonNullable() : inField.getSchema();
      if (nonNullableSchema.getType() != Schema.Type.BYTES || nonNullableSchema.getLogicalType() != null) {
        collector.addFailure(
          String.format("Decrypt field '%s' is of unsupported type '%s'.", inField.getName(),
                        nonNullableSchema.getDisplayName()), "It must be of type bytes.")
          .withConfigElement(Conf.NAME_DECRYPT_FIELDS, inField.getName()).withInputSchemaField(inField.getName());
      }
    }

    for (String decryptField : decryptFields) {
      if (inputSchema.getField(decryptField) == null) {
        collector.addFailure(String.format("Decrypt field '%s' must be present in input schema.", decryptField), null)
          .withConfigElement(Conf.NAME_DECRYPT_FIELDS, decryptField);
      }
    }
  }

  /**
   * Decryptor Plugin config.
   */
  public static class Conf extends KeystoreConf {
    private static final String NAME_SCHEMA = "schema";
    private static final String NAME_DECRYPT_FIELDS = "decryptFields";

    @Description("The fields to decrypt, separated by commas")
    private String decryptFields;

    @Description("Schema to pull fields from")
    @Macro
    private String schema;

    private Set<String> getDecryptFields() {
      Set<String> set = new HashSet<>();
      for (String field : Splitter.on(',').trimResults().split(decryptFields)) {
        set.add(field);
      }
      return ImmutableSet.copyOf(set);
    }

    @Nullable
    private Schema getSchema(FailureCollector collector) {
      try {
        return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        collector.addFailure("Invalid schema : " + e.getMessage(), null).withConfigProperty(NAME_SCHEMA);
      }
      throw collector.getOrThrowException();
    }
  }
}
