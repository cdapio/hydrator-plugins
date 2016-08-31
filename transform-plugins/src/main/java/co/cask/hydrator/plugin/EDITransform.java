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

/**
 * ETL Transform.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("EDIParser")
@Description("Transform to convert data in EDI format")
public class EDITransform<T> extends Transform<StructuredRecord, StructuredRecord> {

  private EDITransformConfig ediTransformConfig;

  public EDITransform(EDITransformConfig ediTransformConfig) {
    this.ediTransformConfig = ediTransformConfig;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    // Perform transformation
    //emitter.emit(value);
  }

  public static class EDITransformConfig extends PluginConfig {
    public EDITransformConfig() {
    }
  }

}

