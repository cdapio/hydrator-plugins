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

package io.cdap.plugin.db.socketfactory.mysql;

import com.google.common.base.Strings;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.SocketConnection;

import io.cdap.plugin.db.socketfactory.BytesTrackingSocket;
import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A MySQL socket factory that keeps track of the number of bytes written.
 */
public class SocketFactory implements com.mysql.cj.protocol.SocketFactory {
  private static String delegateClass;
  private static AtomicLong bytesWritten = new AtomicLong(0);

  private Socket socket;

  public static void setDelegateClass(String name) {
    delegateClass = name;
  }

  public static long getBytesWritten() {
    return bytesWritten.get();
  }

  @Override
  public <T extends Closeable> T connect(
    String host, int portNumber, PropertySet props, int loginTimeout) throws IOException {
    return connect(host, portNumber, props.exposeAsProperties(), loginTimeout);
  }

  /**
   * Implements the interface for com.mysql.cj.protocol.SocketFactory for mysql-connector-java prior
   * to version 8.0.13. This change is required for backwards compatibility.
   */
  public <T extends Closeable> T connect(
    String hostname, int portNumber, Properties props, int loginTimeout) throws IOException {
    if (Strings.isNullOrEmpty(delegateClass)) {
      Socket delegate = javax.net.SocketFactory.getDefault().createSocket(hostname, portNumber);
      socket = new BytesTrackingSocket(delegate, bytesWritten);
    } else {
      try {
        com.google.cloud.sql.mysql.SocketFactory fac = (com.google.cloud.sql.mysql.SocketFactory)
          Class.forName(delegateClass).newInstance();
        Socket delegate = fac.connect(hostname, portNumber, props, loginTimeout);
        socket = new BytesTrackingSocket(delegate, bytesWritten);
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        throw new IOException(String.format("Could not instantiate class %s", delegateClass), e);
      }
    }
    return (T) socket;
  }

  // Cloud SQL sockets always use TLS and the socket returned by connect above is already TLS-ready.
  // It is fine to implement these as no-ops.
  @Override
  public void beforeHandshake() {
  }

  @Override
  public <T extends Closeable> T performTlsHandshake(
    SocketConnection socketConnection, ServerSession serverSession) throws IOException {
    @SuppressWarnings("unchecked")
    T sock = (T) socketConnection.getMysqlSocket();
    return sock;
  }

  @Override
  public void afterHandshake() {
  }
}
