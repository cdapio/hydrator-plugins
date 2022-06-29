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

package io.cdap.plugin.format;

import io.cdap.cdap.etl.api.validation.DelegatingSeekableInputStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

/**
 * InputFile that comes from a Hadoop FileSystem.
 */
public class FileSystemSeekableInputStream extends DelegatingSeekableInputStream {
  private final FSDataInputStream inputStream;

  public FileSystemSeekableInputStream(FSDataInputStream inputStream) {
    super(inputStream);
    this.inputStream = inputStream;
  }

  @Override
  public void seek(long l) throws IOException {
    inputStream.seek(l);
  }

  @Override
  public long getPos() throws IOException {
    return inputStream.getPos();
  }
}
