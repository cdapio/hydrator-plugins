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

package co.cask.hydrator.plugin.sink;


import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Tests for {@link GCSBatchSink} configuration.
 */
public class GCSBatchSinkConfigTest {
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  @Test
  public void testFileSystemProperties() {
    String projectId = "projectId";
    String jsonKey = "jsonKey";
    String path = "path";
    String schema = "schema";
    String bucket = "bucket";
    // Test default properties
    GCSAvroBatchSink.GCSAvroSinkConfig gcsAvroConfig =
      new GCSAvroBatchSink.GCSAvroSinkConfig("gcstest", bucket, schema, projectId, jsonKey, null, path);
    Map<String, String> fsProperties = GSON.fromJson(gcsAvroConfig.getFileSystemProperties(null, projectId, jsonKey),
                                                     MAP_STRING_STRING_TYPE);
    Assert.assertNotNull(fsProperties);
    Assert.assertEquals(5, fsProperties.size());
    Assert.assertEquals("gs://bucket/path", fsProperties.get(FileOutputFormat.OUTDIR));
    Assert.assertEquals(projectId, fsProperties.get("fs.gs.project.id"));
    Assert.assertEquals(jsonKey, fsProperties.get("google.cloud.auth.service.account.json.keyfile"));
  }
}

