/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Transform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Identity Transform copies the source data into the destination without any change.
 */
@Plugin(type = "transform")
@Name("Identity")
@Description("Copies the source data into the destination data without change.")
public final class Identity extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(Identity.class);
  private final Config config;

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public Identity(Config config) {
    this.config = config;
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(in);
  }

  public static class Config extends PluginConfig {
    @Name("schema")
    @Description("Specifies the output schema")
    private final String schema;
    
    public Config(String schema) {
      this.schema = schema;
    }
  }
}

