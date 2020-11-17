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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link Socket} that keeps track of the number of bytes written.
 */
public class BytesTrackingSocket extends Socket {
  private Socket delegate;
  BytesTrackingOutputStream outputStream;
  AtomicLong bytesWritten;

  public BytesTrackingSocket(Socket delegate, AtomicLong bytesWritten) throws IOException {
    this.delegate = delegate;
    this.bytesWritten = bytesWritten;
    if (delegate.isConnected()) {
      this.outputStream = new BytesTrackingOutputStream(delegate.getOutputStream(), bytesWritten);
    }
  }

  @Override
  public void bind(SocketAddress bindpoint) throws IOException {
    delegate.bind(bindpoint);
  }

  @Override
  public synchronized void close() throws IOException {
    delegate.close();
  }

  @Override
  public void connect(SocketAddress endpoint) throws IOException {
    delegate.connect(endpoint);
    if (outputStream == null) {
      outputStream = new BytesTrackingOutputStream(delegate.getOutputStream(), bytesWritten);
    }
  }

  @Override
  public void connect(SocketAddress endpoint, int timeout) throws IOException {
    delegate.connect(endpoint, timeout);
    if (outputStream == null) {
      outputStream = new BytesTrackingOutputStream(delegate.getOutputStream(), bytesWritten);
    }
  }

  @Override
  public SocketChannel getChannel() {
    return delegate.getChannel();
  }

  @Override
  public InetAddress getInetAddress() {
    return delegate.getInetAddress();
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return delegate.getInputStream();
  }

  @Override
  public boolean getKeepAlive() throws SocketException {
    return delegate.getKeepAlive();
  }

  @Override
  public InetAddress getLocalAddress() {
    return delegate.getLocalAddress();
  }

  @Override
  public int getLocalPort() {
    return delegate.getLocalPort();
  }

  @Override
  public SocketAddress getLocalSocketAddress() {
    return delegate.getLocalSocketAddress();
  }

  @Override
  public boolean getOOBInline() throws SocketException {
    return delegate.getOOBInline();
  }

  @Override
  public OutputStream getOutputStream() {
    return outputStream;
  }

  @Override
  public int getPort() {
    return delegate.getPort();
  }

  @Override
  public int getReceiveBufferSize() throws SocketException {
    return delegate.getReceiveBufferSize();
  }

  @Override
  public SocketAddress getRemoteSocketAddress() {
    return delegate.getRemoteSocketAddress();
  }

  @Override
  public boolean getReuseAddress() throws SocketException {
    return delegate.getReuseAddress();
  }

  @Override
  public int getSendBufferSize() throws SocketException {
    return delegate.getSendBufferSize();
  }

  @Override
  public int getSoLinger() throws SocketException {
    return delegate.getSoLinger();
  }

  @Override
  public int getSoTimeout() throws SocketException {
    return delegate.getSoTimeout();
  }

  @Override
  public boolean getTcpNoDelay() throws SocketException {
    return delegate.getTcpNoDelay();
  }

  @Override
  public int getTrafficClass() throws SocketException {
    return delegate.getTrafficClass();
  }

  @Override
  public boolean isBound() {
    return delegate.isBound();
  }

  @Override
  public boolean isClosed() {
    return delegate.isClosed();
  }

  @Override
  public boolean isConnected() {
    return delegate.isConnected();
  }

  @Override
  public boolean isInputShutdown() {
    return delegate.isInputShutdown();
  }

  @Override
  public boolean isOutputShutdown() {
    return delegate.isOutputShutdown();
  }

  @Override
  public void sendUrgentData(int data) throws IOException {
    delegate.sendUrgentData(data);
  }

  @Override
  public void setKeepAlive(boolean on) throws SocketException {
    delegate.setKeepAlive(on);
  }

  @Override
  public void setOOBInline(boolean on) throws SocketException {
    delegate.setOOBInline(on);
  }

  @Override
  public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
    delegate.setPerformancePreferences(connectionTime, latency, bandwidth);
  }

  @Override
  public void setReceiveBufferSize(int size) throws SocketException {
    delegate.setReceiveBufferSize(size);
  }

  @Override
  public void setReuseAddress(boolean on) throws SocketException {
    delegate.setReuseAddress(on);
  }

  @Override
  public void setSendBufferSize(int size) throws SocketException {
    delegate.setSendBufferSize(size);
  }

  @Override
  public void setSoLinger(boolean on, int linger) throws SocketException {
    delegate.setSoLinger(on, linger);
  }

  @Override
  public void setSoTimeout(int timeout) throws SocketException {
    delegate.setSoTimeout(timeout);
  }

  @Override
  public void setTcpNoDelay(boolean on) throws SocketException {
    delegate.setTcpNoDelay(on);
  }

  @Override
  public void setTrafficClass(int tc) throws SocketException {
    delegate.setTrafficClass(tc);
  }

  @Override
  public void shutdownInput() throws IOException {
    delegate.shutdownInput();
  }

  @Override
  public void shutdownOutput() throws IOException {
    delegate.shutdownOutput();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }
}
