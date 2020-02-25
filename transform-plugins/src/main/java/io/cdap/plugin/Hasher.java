/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.plugin.common.TransformLineageRecorderUtils;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Hash field(s) values using one the the digest algorithms.
 */
@Plugin(type = "transform")
@Name("Hasher")
@Description("Encodes field values using one of the digest algorithms. MD2, MD5, SHA1, SHA256, " +
  "SHA384 and SHA512 are the supported message digest algorithms.")
public final class Hasher extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;
  private Set<String> fieldSet = new HashSet<>();

  // For testing purpose only.
  public Hasher(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    config.validate(stageConfigurer.getInputSchema(), stageConfigurer.getFailureCollector());
    stageConfigurer.getFailureCollector().getOrThrowException();
    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    FailureCollector failureCollector = context.getFailureCollector();
    config.validate(context.getInputSchema(), failureCollector);
    failureCollector.getOrThrowException();
    if (context.getInputSchema() == null || context.getInputSchema().getFields() == null) {
      return;
    }

    // Set a list of operations only for the fields in inputSchema and with type string, and identity for
    // the non-string ones present in the output.
    List<String> hashedFields = context.getInputSchema().getFields().stream()
      .filter(field -> config.getFields()
        .contains(field.getName()) && field.getSchema().getType() == Schema.Type.STRING)
      .map(Schema.Field::getName).collect(Collectors.toList());

    List<String> identityFields = TransformLineageRecorderUtils.getFields(context.getInputSchema());
    identityFields.removeAll(hashedFields);

    List<FieldOperation> output = new ArrayList<>();
    output.addAll(TransformLineageRecorderUtils.generateOneToOnes(hashedFields, "hash",
      "Used the digest algorithm to hash the fields."));
    output.addAll(TransformLineageRecorderUtils.generateOneToOnes(identityFields, "identity",
      TransformLineageRecorderUtils.IDENTITY_TRANSFORM_DESCRIPTION));
    context.record(output);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    fieldSet = config.getFields();
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(in.getSchema());
    
    List<Schema.Field> fields = in.getSchema().getFields();
    for (Schema.Field field : fields) {
      String name = field.getName();
      if (fieldSet.contains(name) && field.getSchema().getType() == Schema.Type.STRING) {
        String value = in.get(name);
        String digest = value;
        switch(config.hash.toLowerCase()) {
          case "md2":
            digest = DigestUtils.md2Hex(value);
            break;
          case "md5":
            digest = DigestUtils.md5Hex(value);
            break;
          case "sha1":
            digest = DigestUtils.sha1Hex(value);
            break;
          case "sha256":
            digest = DigestUtils.sha256Hex(value);
            break;
          case "sha384":
            digest = DigestUtils.sha384Hex(value);
            break;
          case "sha512":
            digest = DigestUtils.sha512Hex(value);
            break;
        }
        builder.set(name, digest);
      } else {
        builder.set(name, in.get(name));
      }
    }
    emitter.emit(builder.build());
  }

  /**
   * Hasher Plugin Config.
   */
  public static class Config extends PluginConfig {
    private static final String HASH = "hash";
    private static final String FIELDS = "fields";

    @Name(HASH)
    @Description("Specifies the Hash method for hashing fields.")
    @Nullable
    private final String hash;
    
    @Name(FIELDS)
    @Description("List of fields to hash. Only string fields are allowed")
    private final String fields;
    
    public Config(String hash, String fields) {
      this.hash = hash;
      this.fields = fields;
    }

    private void validate(@Nullable Schema inputSchema, FailureCollector failureCollector) {
      // Checks if hash specified is one of the supported types.
      if (hash != null && !hash.equalsIgnoreCase("md2") && !hash.equalsIgnoreCase("md5") &&
        !hash.equalsIgnoreCase("sha1") && !hash.equalsIgnoreCase("sha256") &&
        !hash.equalsIgnoreCase("sha384") && !hash.equalsIgnoreCase("sha512")) {
        failureCollector.addFailure(String.format("Invalid hasher '%s' specified.", hash),
                                    "Allowed hashers are md2, md5, sha1, sha256, sha384 and sha512");
      }

      if (inputSchema == null) {
        return;
      }

      for (String field : getFields()) {
        Schema.Field inputField = inputSchema.getField(field);
        if (inputField == null) {
          continue;
        }
        Schema inputFieldSchema = inputField.getSchema();
        inputFieldSchema = inputFieldSchema.isNullable() ? inputFieldSchema.getNonNullable() : inputFieldSchema;
        if (inputFieldSchema.getType() != Schema.Type.STRING) {
          failureCollector.addFailure(
            String.format("Field '%s' is of unsupported type '%s'.", field, inputFieldSchema.getDisplayName()),
            "Ensure all fields to hash are strings.")
            .withConfigElement(FIELDS, field);
        }
      }
    }

    private Set<String> getFields() {
      return Arrays.stream(fields.split(",")).map(String::trim).collect(Collectors.toSet());
    }
  }
}

