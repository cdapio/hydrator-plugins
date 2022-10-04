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

import javax.annotation.Nullable;

/**
 * Represents a dataset with a FQN which is a fully-qualified unique identifier for a dataset
 * and the location of the dataset.
 */
public class Asset {

  private static final String DEFAULT_LOCATION = "global";

  private final String referenceName;
  private final String fqn;
  private final String location;

  private Asset(String referenceName, @Nullable String fqn, @Nullable String location) {
    this.referenceName = referenceName;
    this.fqn = fqn == null ? referenceName : fqn;
    this.location = location == null ? DEFAULT_LOCATION : location;
  }

  /**
   * @return the reference name or the normalized FQN of the {@link Asset}
   */
  public String getReferenceName() {
    return referenceName;
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
      "referenceName='" + referenceName + '\'' +
      ", fqn='" + fqn + '\'' +
      ", location='" + location + '\'' +
      '}';
  }

  public static Asset.Builder builder(String referenceName) {
    return new Builder(referenceName);
  }

  /**
   * A builder to create {@link Asset} instance.
   */
  public static final class Builder {
    private final String referenceName;
    private String fqn;
    private String location;

    private Builder(String referenceName) {
      this.referenceName = referenceName;
    }

    /**
     * Set the ID of the program that created the run.
     */
    public Builder setFqn(String fqn) {
      this.fqn = fqn;
      return this;
    }

    /**
     * Set the ID of the program that created the run.
     */
    public Builder setLocation(String location) {
      this.location = location;
      return this;
    }

    /**
     * Creates a new instance of {@link Asset}.
     */
    public Asset build() {
      return new Asset(referenceName, fqn, location);
    }
  }
}
