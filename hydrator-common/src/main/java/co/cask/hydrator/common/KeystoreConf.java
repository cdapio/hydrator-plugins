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

package co.cask.hydrator.common;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.plugin.PluginConfig;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;

/**
 * Config for keystore properties.
 */
public class KeystoreConf extends PluginConfig {
  @Description("transformation algorithm/mode/padding. For example, AES/CBC/PKCS5Padding.")
  private String transformation;

  @Nullable
  @Description("initialization vector if using CBC mode.")
  private String ivHex;

  @Description("path to the keystore on local disk. The keystore must be present on every node in the cluster.")
  private String keystorePath;

  @Nullable
  @Description("password for the keystore.")
  private String keystorePassword;

  @Description("type of keystore. For example, JKS, or JCEKS.")
  private String keystoreType;

  @Description("alias of the key to use in the keystore.")
  private String keyAlias;

  @Description("password for the key to use in the keystore.")
  private String keyPassword;

  public KeystoreConf() {
  }

  @VisibleForTesting
  public KeystoreConf(String transformation, @Nullable String ivHex, String keystorePath, String keystorePassword,
                      String keystoreType, String keyAlias, String keyPassword) {
    this.transformation = transformation;
    this.ivHex = ivHex;
    this.keystorePath = keystorePath;
    this.keystorePassword = keystorePassword;
    this.keystoreType = keystoreType;
    this.keyAlias = keyAlias;
    this.keyPassword = keyPassword;
  }

  public String getTransformation() {
    return transformation;
  }

  @Nullable
  public String getIvHex() {
    return ivHex;
  }

  public String getKeystorePath() {
    return keystorePath;
  }

  public String getKeystorePassword() {
    return keystorePassword;
  }

  public String getKeystoreType() {
    return keystoreType;
  }

  public String getKeyAlias() {
    return keyAlias;
  }

  public String getKeyPassword() {
    return keyPassword;
  }
}
