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

package co.cask.hydrator.plugin.transform;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.LookupConfig;
import co.cask.cdap.etl.api.Transform;
import co.cask.hydrator.plugin.common.StructuredRecordSerializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Transforms records using custom java code provided by the config.
 */
@Plugin(type = "transform")
@Name("Java")
@Description("Executes user provided java code on each record.")
public class JavaTransform extends Transform<StructuredRecord, StructuredRecord> {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(StructuredRecord.class, new StructuredRecordSerializer())
    .create();
  private static final Logger LOG = LoggerFactory.getLogger(JavaTransform.class);

  /**
   * Configuration for the java transform.
   */
  public static class Config extends PluginConfig {
    @Description("Java code defining how to transform input record into zero or more records. " +
      "The code must implement a method " +
      "called 'transform', which takes as input as stringified JSON object (representing the input record) " +
      "emitter object, which can be used to emit records and error messages" +
      "and a context object (which contains CDAP metrics, logger and lookup)" +
      "For example:\n" +
      // TODO example
      "'public void transform(StructuredRecord input, Emitter<StructureRecord> emitter) {\n" +
      "}'\n" +
      "will emit an error if the input id is present in blacklist table, else scale the 'count' field by 1024"
    )
    private final String code;

    @Description("The schema of output objects. If no schema is given, it is assumed that the output schema is " +
      "the same as the input schema.")
    @Nullable
    private final String schema;

    public Config(String code, String schema, LookupConfig lookup) {
      this.code = code;
      this.schema = schema;
    }
  }

  @Override
  public void transform(StructuredRecord structuredRecord, Emitter<StructuredRecord> emitter) throws Exception {

  }
}
