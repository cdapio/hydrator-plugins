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

package co.cask.plugin.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.plugin.etl.batch.HiveConfig;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batch source for Hive.
 */
@Plugin(type = "batchsource")
@Name("Hive")
@Description("Read from an Hive table in batch")
public class HiveBatchSource extends BatchSource<WritableComparable, HCatRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(HiveBatchSource.class);
  private HiveConfig config;

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    LOG.info("Hadoop version: {}", VersionInfo.getVersion());
    Job job = context.getHadoopJob();
    job.setInputFormatClass(HCatInputFormat.class);
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      HCatInputFormat.setInput(job, config.dbName, config.tableName);
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public void transform(KeyValue<WritableComparable, HCatRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
    LOG.info("The HCatRedord is: {}", input.getValue().toString() );
  }
}
