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
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.Field;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.plugin.common.FieldEncryptor;
import io.cdap.plugin.common.KeystoreConf;
import io.cdap.plugin.common.TransformLineageRecorderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.crypto.Cipher;

/**
 * Encrypts record fields.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Encryptor")
@Description("Encrypts fields of records.")
public final class Encryptor extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(Encryptor.class);
  private final Conf conf;
  private Set<String> encryptFields;
  private FieldEncryptor fieldEncryptor;

  public Encryptor(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    encryptFields = conf.getEncryptFields();
    Schema outputSchema = inputSchema == null ? null : getOutputSchema(inputSchema);
    stageConfigurer.setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    encryptFields = conf.getEncryptFields();
    fieldEncryptor = new FileBasedFieldEncryptor(conf, Cipher.ENCRYPT_MODE);
    fieldEncryptor.initialize();
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    encryptFields = conf.getEncryptFields();
    if (context.getInputSchema() == null || context.getInputSchema().getFields() == null) {
      return;
    }

    // Use all encryptFields from conf that also exist in input schema.
    List<String> encryptedFields = TransformLineageRecorderUtils.getFields(context.getInputSchema()).stream()
      .filter(encryptFields::contains)
      .collect(Collectors.toList());

    List<String> identityFields = TransformLineageRecorderUtils.getFields(context.getInputSchema());
    identityFields.removeAll(encryptedFields);

    List<FieldOperation> output = new ArrayList<>();
    output.addAll(TransformLineageRecorderUtils.generateOneToOnes(encryptedFields, "encrypt",
      "Encrypted the requested fields."));
    output.addAll(TransformLineageRecorderUtils.generateOneToOnes(identityFields, "identity",
      TransformLineageRecorderUtils.IDENTITY_TRANSFORM_DESCRIPTION));
    context.record(output);
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    Schema schema = getOutputSchema(in.getSchema());
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    for (Field field : in.getSchema().getFields()) {
      if (encryptFields.contains(field.getName())) {
        recordBuilder.set(field.getName(), fieldEncryptor.encrypt(in.get(field.getName()), field.getSchema()));
      } else {
        recordBuilder.set(field.getName(), in.get(field.getName()));
      }
    }
    emitter.emit(recordBuilder.build());
  }

  private Schema getOutputSchema(Schema schema) {
    List<Field> outputFields = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      if (encryptFields.contains(field.getName())) {
        outputFields.add(Schema.Field.of(field.getName(), Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
      } else {
        outputFields.add(field);
      }
    }
    return Schema.recordOf(schema.getRecordName(), outputFields);
  }

  /**
   * Decryptor Plugin config.
   */
  public static class Conf extends KeystoreConf {
    @Description("The fields to encrypt, separated by commas")
    private String encryptFields;

    private Set<String> getEncryptFields() {
      Set<String> set = new HashSet<>();
      for (String field : Splitter.on(',').trimResults().split(encryptFields)) {
        set.add(field);
      }
      return ImmutableSet.copyOf(set);
    }
  }
}
