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

package co.cask.hydrator.plugin.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;


/**
 * /**
 * {@link GCSBatchSink} that stores the data of the latest run of an adapter in S3.
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */

public abstract class GCSBatchSink<KEY_OUT, VAL_OUT> extends ReferenceBatchSink<StructuredRecord, KEY_OUT, VAL_OUT> {
  public static final String BUCKET_DES = "GCS bucket to use as a default bucket if fs.default.name is not a gs: uri.";
  public static final String PROJECT_ID_DES = "Google Cloud Project ID with access to configured GCS buckets";
  public static final String SERVICE_EMAIL_DES = "The email address is associated with the service " +
                                             "account used for GCS access";
  public static final String SERVICE_KEY_FILE_DES = "The PKCS12 (p12) certificate file of the " +
                                                "service account used for GCS access";

  private final GCSSinkConfig config;
  protected GCSBatchSink(GCSSinkConfig config) {
    super(config);
    this.config = config;
    }

  @Override
  public final void prepareRun(BatchSinkContext context) {

  }

  public static class GCSSinkConfig extends ReferencePluginConfig {

    @Name("Bucket_Key")
    @Description(BUCKET_DES)
    @Macro
    protected String bucketKey;

    @Name("Project_Id")
    @Description(PROJECT_ID_DES)
    @Macro
    protected String projectId;

    @Name("Service_Email")
    @Description(SERVICE_EMAIL_DES)
    @Macro
    protected String serviceEmail;

    @Name("P12_key_file")
    @Description(SERVICE_KEY_FILE_DES)
    @Macro
    protected String p12Key;

    public GCSSinkConfig() {
      super("");
    }

    public GCSSinkConfig(String referenceName, String bucketKey, String projectId, String serviceEmail,
                              String serviceKeyFile) {
      super(referenceName);
    }
  }
}
