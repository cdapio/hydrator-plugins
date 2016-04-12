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

package co.cask.hydrator.plugin.realtime;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.realtime.SourceState;
import co.cask.cdap.test.TestBase;
import co.cask.hydrator.common.test.MockRealtimeContext;
import co.cask.hydrator.plugin.realtime.config.UrlFetchRealtimeSourceConfig;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * <p>
 *  Unit test for {@link UrlFetchRealtimeSource} ETL realtime source class.
 * </p>
 */
public class UrlFetchTest extends TestBase {

  @Test
  public void testUrlFetch() throws Exception {
    Path testFilePath =
      Paths.get(UrlFetchTest.class.getProtectionDomain().getCodeSource().getLocation().getPath() + "/testdata.json");
    UrlFetchRealtimeSourceConfig config = new UrlFetchRealtimeSourceConfig(
      testFilePath.toUri().toString(),
      1L
    );

    UrlFetchRealtimeSource source = new UrlFetchRealtimeSource(config);
    source.initialize(new MockRealtimeContext());

    MockEmitter emitter = new MockEmitter();
    SourceState state = new SourceState();

    source.poll(emitter, state);
    StructuredRecord urlData = emitter.getUrlData();
    Assert.assertNotNull(urlData);
    Assert.assertNotNull(urlData.get("url"));
    Assert.assertNotNull(urlData.get("body"));
  }

  private static class MockEmitter implements Emitter<StructuredRecord> {

    private StructuredRecord urlData;

    @Override
    public void emit(StructuredRecord value) {
      urlData = value;
    }

    @Override
    public void emitError(InvalidEntry<StructuredRecord> value) {
      //no-op
    }

    public StructuredRecord getUrlData() {
      return urlData;
    }

  }
}
