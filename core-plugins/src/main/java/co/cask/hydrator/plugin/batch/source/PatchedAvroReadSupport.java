/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.source;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * To workaround avro classloader issues (CDAP-12875).
 *
 * @param <T> record type
 */
public class PatchedAvroReadSupport<T> extends AvroReadSupport<T> {

  @Override
  public RecordMaterializer<T> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData,
                                              MessageType fileSchema, ReadContext readContext) {
    Map<String, String> metadata = new HashMap<>();
    metadata.putAll(keyValueMetaData);
    metadata.put("parquet.avro.schema", configuration.get("path.tracking.schema"));
    return super.prepareForRead(configuration, Collections.unmodifiableMap(metadata), fileSchema, readContext);
  }
}
