/*
 * Copyright © 2021 Cask Data, Inc.
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
package io.cdap.plugin.utils;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Optional;

/**
 * HttpUtils contains the helper functions.
 */
public class HttpUtils {

  private static final Logger logger = Logger.getLogger(HttpUtils.class);

  public static Optional<String> getHttpResponseBody(
    String url, String method, List<KeyValue> headers, String requestBody, String format)
    throws UnsupportedEncodingException {
    String result = null;
    HttpRequestBase httpRequest = null;
    if (method.equalsIgnoreCase("GET")) {
      httpRequest = new HttpGet(url);
    } else if (method.equalsIgnoreCase("POST")) {
      httpRequest = new HttpPost(url);
      ((HttpPost) httpRequest).setEntity(new StringEntity(requestBody));
    }
    for (KeyValue header : headers) {
      httpRequest.addHeader(header.getKey(), header.getValue());
    }
    if (format.equalsIgnoreCase("json")) {
      httpRequest.setHeader("Accept", "application/json");
      httpRequest.setHeader("Content-type", "application/json");
    }
    try (CloseableHttpClient httpClient = HttpClients.createDefault();
         CloseableHttpResponse response = httpClient.execute(httpRequest)) {
      System.out.println(response.getStatusLine().toString());
      HttpEntity entity = response.getEntity();
      if (entity != null) {
        result = EntityUtils.toString(entity);
        System.out.println(result);
      }
    } catch (IOException e) {
      logger.error("HttpUtils : Error in HTTP request - " + e);
    }
    return Optional.ofNullable(result);
  }
}
