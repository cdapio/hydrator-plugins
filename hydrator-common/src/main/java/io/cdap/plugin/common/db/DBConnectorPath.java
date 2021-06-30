/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.plugin.common.db;

import javax.annotation.Nullable;

/**
 * Database connector path which contains common method about the path
 */
public interface DBConnectorPath {

  /**
   * Return if the path can contain database
   */
  boolean containDatabase();

  /**
   * Return if the path can contain schema
   */
  boolean containSchema();

  /**
   * Get the database in the path, null if it is not there or not supported
   */
  @Nullable
  String getDatabase();

  /**
   * Get the schema in the path, null if it is not there or not supported
   */
  @Nullable
  String getSchema();

  /**
   * Get the table in the path, null if it is not there
   */
  @Nullable
  String getTable();

  /**
   * Return if the path is root or not
   */
  boolean isRoot();
}
