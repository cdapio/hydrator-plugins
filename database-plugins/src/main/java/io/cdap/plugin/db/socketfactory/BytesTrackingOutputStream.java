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

package io.cdap.plugin.db.socketfactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An {@link OutputStream} that keeps track of the number of bytes written.
 */
public class BytesTrackingOutputStream extends OutputStream {
  private OutputStream delegate;
  private AtomicLong bytesWritten;

  public BytesTrackingOutputStream(OutputStream delegate, AtomicLong bytesWritten) {
    this.delegate = delegate;
    this.bytesWritten = bytesWritten;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public void write(int data) throws IOException {
    write(new byte[]{(byte) data}, 0, 1);
  }

  @Override
  public void write(byte[] data) throws IOException {
    write(data, 0, data.length);
  }

  @Override
  public synchronized void write(byte[] data, int offset, int countBytesToWrite) throws IOException {
    delegate.write(data, offset, countBytesToWrite);
    bytesWritten.addAndGet(countBytesToWrite);
  }
}
