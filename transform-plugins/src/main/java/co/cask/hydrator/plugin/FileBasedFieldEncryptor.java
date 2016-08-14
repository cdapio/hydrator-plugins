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

package co.cask.hydrator.plugin;

import co.cask.hydrator.common.FieldEncryptor;
import co.cask.hydrator.common.KeystoreConf;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Reads the keystore from a local file.
 */
public class FileBasedFieldEncryptor extends FieldEncryptor {

  public FileBasedFieldEncryptor(KeystoreConf conf, int mode) {
    super(conf, mode);
  }

  @Override
  public InputStream getKeystoreInputStream(String keystorePath) throws Exception {
    File file = new File(keystorePath);
    return new FileInputStream(file);
  }
}
