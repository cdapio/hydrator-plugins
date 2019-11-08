/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.hydrator.format.output;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.hydrator.common.HiveSchemaConverter;
import co.cask.hydrator.format.StructuredToOrcTransformer;
import org.apache.orc.OrcConf;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Defines logic for reading and writing Orc files.
 */
public class OrcOutputFormatter implements FileOutputFormatter<Void, OrcStruct> {
  private final StructuredToOrcTransformer recordTransformer;
  private final Schema schema;

  public OrcOutputFormatter(Schema schema) {
    this.schema = schema;
    recordTransformer = new StructuredToOrcTransformer();
  }

  @Override
  public KeyValue<Void, OrcStruct> transform(StructuredRecord record) throws IOException {
    return new KeyValue<>(null, recordTransformer.transform(record, schema));
  }

  @Override
  public String getFormatClassName() {
    return OrcOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getFormatConfig() {
    Map<String, String> conf = new HashMap<>();
    StringBuilder builder = new StringBuilder();
    try {
      HiveSchemaConverter.appendType(builder, this.schema);
    } catch (UnsupportedTypeException e) {
      throw new IllegalArgumentException(String.format("Not a valid Schema %s", this.schema), e);
    }
    conf.put(OrcConf.MAPRED_OUTPUT_SCHEMA.getAttribute(), builder.toString());

    // TODO: add all attributes listed in method buildOptions in class org.apache.orc.mapred.OrcOutputFormat

    return conf;
  }
}
