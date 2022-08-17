/*
 * Copyright © 2022 Cask Data, Inc.
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
package io.cdap.plugin.common;

/**
 * Represents a dataset with a FQN which is a fully-qualified unique identifier for a dataset
 * and the location of the dataset.
 */
public class Asset {

  public static final String DEFAULT_LOCATION = "unknown";

  private final String fqn;
  private final String location;

  public Asset(String fqn) {
      this.fqn = fqn;
      this.location = DEFAULT_LOCATION;
  }

  public Asset(String fqn, String location) {
      this.fqn = fqn;
      this.location = location;
  }

  /**
   * @return the fully-qualified name of the {@link Asset}
   */
  public String getFqn() {
      return fqn;
  }

  /**
   * @return the location of the {@link Asset}.
   */
  public String getLocation() {
      return location;
  }

  @Override
  public String toString() {
      return "Asset{" +
        "fqn='" + fqn + '\'' +
        ", location='" + location + '\'' +
        '}';
  }
}
