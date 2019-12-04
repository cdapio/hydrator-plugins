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
import co.cask.cdap.api.data.schema.Schema.Field;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.hydrator.common.FieldEncryptor;
import co.cask.hydrator.common.KeystoreConf;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.crypto.Cipher;
import javax.ws.rs.Path;

/**
 * Encrypts record fields.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Encryptor")
@Description("Encrypts fields of records.")
public final class Encryptor extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(Encryptor.class);
  private final Conf conf;
  private FieldEncryptor fieldEncryptor;

  public Encryptor(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    Schema outputSchema = inputSchema == null ? null : getOutputSchema(inputSchema, conf);
    stageConfigurer.setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    fieldEncryptor = new FileBasedFieldEncryptor(conf, Cipher.ENCRYPT_MODE);
    fieldEncryptor.initialize();
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    Schema schema = getOutputSchema(in.getSchema(), conf);
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
    for (Field field : in.getSchema().getFields()) {
      if (conf.getEncryptFields().contains(field.getName())) {
        recordBuilder.set(field.getName(), fieldEncryptor.encrypt(in.get(field.getName()), field.getSchema()));
      } else {
        recordBuilder.set(field.getName(), in.get(field.getName()));
      }
    }
    emitter.emit(recordBuilder.build());
  }

  @Path("outputSchema")
  public Schema getOutputSchema(GetSchemaRequest request) {
    return getOutputSchema(request.inputSchema, request);
  }

  private Schema getOutputSchema(Schema inputSchema, Conf conf) {
    validategetEncryptFields(conf.getEncryptFields(), inputSchema);
    List<Field> outputFields = new ArrayList<>();
    if (inputSchema != null) {
      for (Schema.Field field : inputSchema.getFields()) {
        if (conf.getEncryptFields().contains(field.getName())) {
          outputFields.add(Schema.Field.of(field.getName(), Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
        } else {
          outputFields.add(field);
        }
      }
    }
    return Schema.recordOf(inputSchema.getRecordName(), outputFields);
  }

  private boolean validategetEncryptFields(Set<String> encryptFields, Schema inputSchema) {
    if (encryptFields != null && !encryptFields.isEmpty()) {
      for (String field: encryptFields) {
        if (inputSchema.getField(field) == null) {
          throw new IllegalArgumentException(String.format("%s is not present in inputSchema.", field));
        }
      }
    }
    return true;
  }

  public static class GetSchemaRequest extends Conf {
    private Schema inputSchema;
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
