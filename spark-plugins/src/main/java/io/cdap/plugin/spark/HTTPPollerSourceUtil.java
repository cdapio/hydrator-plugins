/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.spark;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.plugin.common.http.HTTPPollConfig;
import io.cdap.plugin.common.http.HTTPRequestor;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Util method for {@link HTTPPollerSource}.
 *
 * This class contains methods for {@link HTTPPollerSource} that require spark classes because during validation
 * spark classes are not available. Refer CDAP-15912 for more information.
 */
class HTTPPollerSourceUtil {
  private static final Logger LOG = LoggerFactory.getLogger(HTTPPollerSourceUtil.class);

  static JavaDStream<StructuredRecord> getJavaDStream(StreamingContext context, HTTPPollConfig conf) {
    return context.getSparkStreamingContext().receiverStream(HTTPPollerSourceUtil.getReceiver(conf));
  }

  /**
   * Gets {@link JavaReceiverInputDStream} for {@link HTTPPollerSource}.
   *
   * @param conf {@link HTTPPollConfig} config
   */
  private static Receiver<StructuredRecord> getReceiver(HTTPPollConfig conf) {
    return new Receiver<StructuredRecord>(StorageLevel.MEMORY_ONLY()) {

      @Override
      public StorageLevel storageLevel() {
        return StorageLevel.MEMORY_ONLY();
      }

      @Override
      public void onStart() {
        new Thread() {
          @Override
          public void run() {
            HTTPRequestor httpRequestor = new HTTPRequestor(conf);
            while (!isStopped()) {
              try {
                store(httpRequestor.get());
              } catch (Exception e) {
                LOG.error("Error getting content from {}.", conf.getUrl(), e);
              }

              try {
                TimeUnit.SECONDS.sleep(conf.getInterval());
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            }
          }

          @Override
          public void interrupt() {
            super.interrupt();
          }
        }.start();
      }

      @Override
      public void onStop() {
        // no-op
      }
    };
  }

  private HTTPPollerSourceUtil() {
    // no-op
  }
}
