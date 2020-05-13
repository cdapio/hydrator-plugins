/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin.common.batch;

import io.cdap.plugin.common.CombineClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.function.Function;

/**
 * A utility for providing operations on {@link Job}.
 */
public final class JobUtils {

  /**
   * Creates a new instance of {@link Job}. Note that the job created is not meant for actual MR
   * submission. It's just for setting up configurations.
   */
  public static Job createInstance() throws IOException {
    Job job = Job.getInstance();

    // some input formats require the credentials to be present in the job. We don't know for
    // sure which ones (HCatalog is one of them), so we simply always add them. This has no other
    // effect, because this method is only used at configure time and will be ignored later on.
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      job.getCredentials().addAll(credentials);
    }

    return job;
  }

  /**
   * Calls a {@link Function} with the given {@link JobContext}, with the job classloader sets to a
   * {@link CombineClassLoader}, with the origin job classloader as the parent, and the given extra classloader
   * as the delegate.
   */
  public static <T, E extends Exception> T applyWithExtraClassLoader(JobContext job, ClassLoader extraClassLoader,
                                                                     ThrowableFunction<JobContext, T, E> f) throws E {
    Configuration hConf = job.getConfiguration();
    ClassLoader cl = hConf.getClassLoader();
    hConf.setClassLoader(new CombineClassLoader(cl, extraClassLoader));
    try {
      return f.apply(job);
    } finally {
      hConf.setClassLoader(cl);
    }
  }

  private JobUtils() {
    // no-op
  }
}
