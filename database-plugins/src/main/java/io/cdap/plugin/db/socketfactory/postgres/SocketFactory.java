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

package io.cdap.plugin.db.socketfactory.postgres;

import com.google.common.base.Strings;
import io.cdap.plugin.db.socketfactory.BytesTrackingSocket;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Postgres socket factory that keeps track of the number of bytes written.
 */
public class SocketFactory extends javax.net.SocketFactory {
  private static String delegateClass;
  private static AtomicLong bytesWritten = new AtomicLong(0);

  private Properties properties;
  private String instanceName;

  public static void setDelegateClass(String name) {
    delegateClass = name;
  }

  public static long getBytesWritten() {
    return bytesWritten.get();
  }

  public SocketFactory(Properties properties) {
    this.properties = properties;
  }

  @Deprecated
  public SocketFactory(String instanceName) {
    this.instanceName = instanceName;
  }

  @Override
  public Socket createSocket() throws IOException {
    try {
      if (!Strings.isNullOrEmpty(delegateClass)) {
        Class<?> cl = Class.forName(delegateClass);
        com.google.cloud.sql.postgres.SocketFactory fac = null;
        if (properties != null) {
          Constructor<?> cons = cl.getConstructor(Properties.class);
          fac = (com.google.cloud.sql.postgres.SocketFactory) cons.newInstance(properties);
        } else {
          Constructor<?> cons = cl.getConstructor(String.class);
          fac = (com.google.cloud.sql.postgres.SocketFactory) cons.newInstance(instanceName);
        }
        Socket delegate = fac.createSocket();
        return new BytesTrackingSocket(delegate, bytesWritten);
      } else {
        Socket delegate = javax.net.SocketFactory.getDefault().createSocket();
        return new BytesTrackingSocket(delegate, bytesWritten);
      }
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException |
      NoSuchMethodException e) {
      throw new IOException(String.format("Could not instantiate class %s", delegateClass), e);
    }
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException {
    Socket delegate = javax.net.SocketFactory.getDefault().createSocket(host, port);
    return new BytesTrackingSocket(delegate, bytesWritten);
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
    throws IOException {
    Socket delegate = javax.net.SocketFactory.getDefault().createSocket(host, port, localHost, localPort);
    return new BytesTrackingSocket(delegate, bytesWritten);
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException {
    Socket delegate = javax.net.SocketFactory.getDefault().createSocket(host, port);
    return new BytesTrackingSocket(delegate, bytesWritten);
  }

  @Override
  public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
    throws IOException {
    Socket delegate = javax.net.SocketFactory.getDefault().createSocket(address, port, localAddress, localPort);
    return new BytesTrackingSocket(delegate, bytesWritten);
  }

}
