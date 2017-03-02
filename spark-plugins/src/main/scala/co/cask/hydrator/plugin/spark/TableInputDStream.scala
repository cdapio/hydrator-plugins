/*
 *
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

package co.cask.hydrator.plugin.spark

import co.cask.cdap.api.dataset.table.Row
import co.cask.cdap.api.spark.JavaSparkExecutionContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{StreamingContext, Time}

/**
 * An InputDStream that periodically refreshes its contents. Each interval, it simply returns the table state
 * since the last refresh.
 *
 * @param sec the CDAP spark execution context
 * @param ssc the Spark streaming context
 * @param name the table name
 */
class TableInputDStream(sec: JavaSparkExecutionContext,
                         ssc: StreamingContext,
                         name: String,
                         refreshInterval: Long,
                         @transient var lastRefreshTime: Long = 0,
                         @transient var table: RDD[(Array[Byte], Row)] = null)
  extends InputDStream[(Array[Byte], Row)](ssc: StreamingContext) {

  override def start(): Unit = {
    lastRefreshTime = 0
    // no-op
  }

  override def stop(): Unit = {
    // no-op
  }

  override def compute(validTime: Time): Option[RDD[(Array[Byte], Row)]] = {
    refreshIfNeeded(validTime.milliseconds)
    Some(table)
  }

  def refreshIfNeeded(currentTime: Long): Unit = {
    val threshold: Long = lastRefreshTime + refreshInterval - lastRefreshTime % refreshInterval
    if (lastRefreshTime == 0 || currentTime > threshold) {
      table = sec.fromDataset(name).rdd.cache()
      lastRefreshTime = currentTime
    }
  }
}