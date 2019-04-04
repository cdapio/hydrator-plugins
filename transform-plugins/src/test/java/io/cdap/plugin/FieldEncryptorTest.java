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

package io.cdap.plugin;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.FieldEncryptor;
import io.cdap.plugin.common.KeystoreConf;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.security.KeyStore;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

/**
 */
public class FieldEncryptorTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static KeystoreConf keystoreConf;

  @BeforeClass
  public static void setupKeyStore() throws Exception {
    File keystoreFile = TMP_FOLDER.newFile("keystore.jceks");
    keystoreConf = new KeystoreConf("AES/CBC/PKCS5Padding",
                                    "22BA219FC88FC0826CCAC88C474801D3",
                                    keystoreFile.getAbsolutePath(),
                                    "myKeystorePassword",
                                    "JCEKS",
                                    "mySecretKey",
                                    "mySecretKeyPassword");

    KeyStore ks = KeyStore.getInstance(keystoreConf.getKeystoreType());
    ks.load(null);
    SecretKey secretKey = KeyGenerator.getInstance("AES").generateKey();
    KeyStore.SecretKeyEntry skEntry = new KeyStore.SecretKeyEntry(secretKey);
    ks.setEntry(keystoreConf.getKeyAlias(), skEntry,
                new KeyStore.PasswordProtection(keystoreConf.getKeyPassword().toCharArray()));

    try (FileOutputStream fos = new FileOutputStream(keystoreFile)) {
      ks.store(fos, keystoreConf.getKeystorePassword().toCharArray());
    }
  }

  @Test
  public void testEncryption() throws Exception {
    FieldEncryptor encryptor = new FileBasedFieldEncryptor(keystoreConf, Cipher.ENCRYPT_MODE);
    encryptor.initialize();
    FieldEncryptor decryptor = new FileBasedFieldEncryptor(keystoreConf, Cipher.DECRYPT_MODE);
    decryptor.initialize();

    Schema fieldSchema = Schema.of(Schema.Type.STRING);
    Assert.assertEquals("abc", decryptor.decrypt(encryptor.encrypt("abc", fieldSchema), fieldSchema));
  }

}
