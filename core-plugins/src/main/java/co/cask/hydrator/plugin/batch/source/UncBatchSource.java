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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import org.apache.hadoop.io.LongWritable;



/**
 * Created by Abhinav on 8/30/16.
 */
public class UncBatchSource extends ReferenceBatchSource<LongWritable, Object, StructuredRecord> {

  public UncBatchSource(ReferencePluginConfig config) {
    super(config);
  }

  @Override
  public void prepareRun(BatchSourceContext batchSourceContext) throws Exception {

  }


  public static class Uncconfig extends ReferencePluginConfig {


    @Macro
    public String path;

    public Uncconfig(String referenceName, String path) {
      super(referenceName);
      this.path = path;
    }

    public Uncconfig(String referenceName) {
      super(referenceName);
    }
  }
}
