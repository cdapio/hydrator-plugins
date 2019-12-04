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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.plugin.PluginConfig;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;

/**
 * Config for keystore properties.
 */
public class KeystoreConf extends PluginConfig {
  @Description("Transformation algorithm, mode, and padding, separated by slashes; for example: AES/CBC/PKCS5Padding")
  @Macro
  private String transformation;

  @Nullable
  @Description("Initialization vector if using CBC mode")
  @Macro
  private String ivHex;

  @Description("Path to the keystore on local disk; the keystore must be present on every node of the cluster")
  @Macro
  private String keystorePath;

  @Nullable
  @Description("Password for the keystore")
  @Macro
  private String keystorePassword;

  @Description("Type of keystore; for example: JKS or JCEKS")
  @Macro
  private String keystoreType;

  @Description("Alias of the key to use in the keystore")
  @Macro
  private String keyAlias;

  @Description("Password for the key to use in the keystore")
  @Macro
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
    return KeyStoreUtil.getValidPath(keystorePath);
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
