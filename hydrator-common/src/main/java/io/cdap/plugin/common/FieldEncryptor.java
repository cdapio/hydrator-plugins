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

package io.cdap.plugin.common;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.commons.codec.binary.Hex;

import java.io.InputStream;
import java.security.Key;
import java.security.KeyStore;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.spec.IvParameterSpec;

/**
 * Encrypts and decrypts fields based on their schema.
 */
public abstract class FieldEncryptor {
  private final KeystoreConf conf;
  private int mode;
  private Cipher cipher;

  public FieldEncryptor(KeystoreConf conf, int mode) {
    this.mode = mode;
    this.conf = conf;
  }

  public void initialize() throws Exception {
    KeyStore keystore = KeyStore.getInstance(conf.getKeystoreType());
    try (InputStream keystoreStream = getKeystoreInputStream(conf.getKeystorePath())) {
      keystore.load(keystoreStream, conf.getKeystorePassword().toCharArray());
    }
    Key key = keystore.getKey(conf.getKeyAlias(), conf.getKeyPassword().toCharArray());
    cipher = Cipher.getInstance(conf.getTransformation());
    if (conf.getIvHex() != null) {
      byte[] ivBytes = Hex.decodeHex(conf.getIvHex().toCharArray());
      IvParameterSpec ivParameterSpec = new IvParameterSpec(ivBytes);
      cipher.init(mode, key, ivParameterSpec);
    } else {
      cipher.init(mode, key);
    }
  }

  public abstract InputStream getKeystoreInputStream(String keystorePath) throws Exception;

  public byte[] encrypt(Object fieldVal, Schema fieldSchema) throws BadPaddingException, IllegalBlockSizeException {
    if (fieldVal == null) {
      return null;
    }

    Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    byte[] fieldBytes;
    switch (fieldType) {
      case INT:
        fieldBytes = Bytes.toBytes((int) fieldVal);
        break;
      case LONG:
        fieldBytes = Bytes.toBytes((long) fieldVal);
        break;
      case FLOAT:
        fieldBytes = Bytes.toBytes((float) fieldVal);
        break;
      case DOUBLE:
        fieldBytes = Bytes.toBytes((double) fieldVal);
        break;
      case STRING:
        fieldBytes = Bytes.toBytes((String) fieldVal);
        break;
      case BYTES:
        fieldBytes = (byte[]) fieldVal;
        break;
      default:
        throw new IllegalArgumentException("field type " + fieldType + " is not supported.");
    }
    return cipher.doFinal(fieldBytes);
  }

  public Object decrypt(byte[] fieldBytes, Schema fieldSchema) throws BadPaddingException, IllegalBlockSizeException {
    if (fieldBytes == null) {
      return null;
    }

    Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    fieldBytes = cipher.doFinal(fieldBytes);
    switch (fieldType) {
      case INT:
        return Bytes.toInt(fieldBytes);
      case LONG:
        return Bytes.toLong(fieldBytes);
      case FLOAT:
        return Bytes.toFloat(fieldBytes);
      case DOUBLE:
        return Bytes.toDouble(fieldBytes);
      case STRING:
        return Bytes.toString(fieldBytes);
      case BYTES:
        return fieldBytes;
      default:
        throw new IllegalArgumentException("field type " + fieldType + " is not supported.");
    }
  }

}
