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

package co.cask.hydrator.plugin.realtime.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.realtime.DataWriter;
import co.cask.cdap.etl.api.realtime.RealtimeSink;

/**
 * RealTime sink which writes to AWS Kinesis streams
 */
@Plugin(type = RealtimeSink.PLUGIN_TYPE)
@Name("KinesisStream")
@Description("Real-time Sink for AWS Kinesis streams")
public class RealTimeKinesisStreamSink extends RealtimeSink<StructuredRecord> {

  @Override
  public int write(Iterable<StructuredRecord> iterable, DataWriter dataWriter) throws Exception {
    return 0;
  }
}
