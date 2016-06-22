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

package co.cask.hydrator.common.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * A utility for providing operations on {@link Job}.
 */
public final class JobUtils {

  public static final String MAPR_FS_IMPLEMENTATION_KEY = "fs.maprfs.impl";

  /**
   * Creates a new instance of {@link Job}. Note that the job created is not meant for actual MR
   * submission. It's just for setting up configurations.
   */
  public static Job createInstance() throws IOException {
    Job job = Job.getInstance();
    Configuration conf = job.getConfiguration();
    // Remember the values of the default filesystem and implementation of the filesystem
    // for MapR clusters. The values are used by FileInputFormat class while configuring the job.
    String fsDefaultURI = conf.get(FileSystem.FS_DEFAULT_NAME_KEY);
    String maprfsImplValue = conf.get(MAPR_FS_IMPLEMENTATION_KEY);
    conf.clear();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, fsDefaultURI);
    if (maprfsImplValue != null) {
      conf.set(MAPR_FS_IMPLEMENTATION_KEY, maprfsImplValue);
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      // If runs in secure cluster, this program runner is running in a yarn container, hence not able
      // to get authenticated with the history.
      conf.unset("mapreduce.jobhistory.address");
      conf.setBoolean(Job.JOB_AM_ACCESS_DISABLED, false);

      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      job.getCredentials().addAll(credentials);
    }

    return job;
  }

  private JobUtils() {
    // no-op
  }
}
