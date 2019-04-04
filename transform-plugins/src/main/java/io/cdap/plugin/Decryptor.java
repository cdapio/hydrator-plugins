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

package io.cdap.plugin;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.Field;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.plugin.common.FieldEncryptor;
import io.cdap.plugin.common.KeystoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.crypto.Cipher;

/**
 * Decrypts record fields.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Decryptor")
@Description("Decrypts fields of records.")
public final class Decryptor extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(Decryptor.class);
  private final Conf conf;
  private Set<String> decryptFields;
  private Schema schema;
  private FieldEncryptor fieldEncryptor;

  public Decryptor(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    pipelineConfigurer.getStageConfigurer().setOutputSchema(conf.getSchema());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    schema = conf.getSchema();
    decryptFields = conf.getDecryptFields();
    fieldEncryptor = new FileBasedFieldEncryptor(conf, Cipher.DECRYPT_MODE);
    fieldEncryptor.initialize();
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

  /**
   * Decryptor Plugin config.
   */
  public static class Conf extends KeystoreConf {
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

    private Schema getSchema() {
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException(String.format("Error parsing schema %s. Reason: %s",
                                                         schema, e.getMessage()));
      }
    }
  }
}
