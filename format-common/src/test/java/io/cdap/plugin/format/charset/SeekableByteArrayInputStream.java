/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.format.charset;

import org.apache.hadoop.fs.Seekable;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class SeekableByteArrayInputStream extends ByteArrayInputStream implements Seekable {
  public SeekableByteArrayInputStream(byte[] buf) {
    super(buf);
  }

  @Override
  public void seek(long pos) throws IOException {
    super.reset();
    super.skip(pos);
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new UnsupportedOperationException();
  }
}
