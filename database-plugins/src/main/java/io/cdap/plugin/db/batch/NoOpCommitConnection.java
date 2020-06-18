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

package io.cdap.plugin.db.batch;

import java.sql.Connection;

/**
 * A hack to work around jdbc drivers that create connections that don't support commit.
 * This is true of the Hive jdbc driver. Delegates all operations except commit, which is a no-op.
 */
public class NoOpCommitConnection extends ForwardingConnection {

  public NoOpCommitConnection(Connection delegate) {
    super(delegate);
  }

  @Override
  public void commit() {
    // no-op
  }
}
