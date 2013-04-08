/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.rest;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * REST client used for interacting with ElasticSearch. Performs basic
 * operations; for buffer/batching operation consider using BufferedRestClient.
 */
public class RestClient implements Closeable {

  private static final Log log = LogFactory.getLog(RestClient.class);

  private HttpClient client;
  private final String target;
  private ObjectMapper mapper = new ObjectMapper();

  public RestClient(String targetUri) {
    this(targetUri, null);
  }
  
  public RestClient(String targetUri, String objectMapperClass) {

    if (objectMapperClass == null) {
      mapper = new ObjectMapper();
    } else {
      mapper = createObjectMapper(objectMapperClass);
    }

    HttpClientParams params = new HttpClientParams();
    params.setConnectionManagerTimeout(60 * 1000);

    client = new HttpClient(params);
    HostConfiguration hostConfig = new HostConfiguration();
    try {
      hostConfig.setHost(new URI(targetUri, false));
    } catch (IOException ex) {
      throw new IllegalArgumentException("Invalid target URI " + targetUri, ex);
    }
    client.setHostConfiguration(hostConfig);
    this.target = targetUri;
  }

  private ObjectMapper createObjectMapper(String objectMapperClass) {
    try {
      @SuppressWarnings("unchecked")
      Class<ObjectMapper> cutomObjectMapper = (Class<ObjectMapper>) Class.forName(objectMapperClass);
      ObjectMapper objectMapper = cutomObjectMapper.newInstance();
      return objectMapper;
    } catch (ClassNotFoundException ex) {
      throw new IllegalArgumentException("Missing custom ObjectMapper class " + objectMapperClass, ex);
    } catch (InstantiationException ex) {
      throw new IllegalStateException("The ObjectMapper class or its nullary constructor is not accessible:"
          + objectMapperClass, ex);
    } catch (IllegalAccessException ex) {
      throw new IllegalStateException("Cannot instantiate  an abstract class or interface ObjectMapper for class:"
          + objectMapperClass, ex);
    }
  }

  /**
   * Queries ElasticSearch using pagination.
   * 
   * @param uri
   *          query to execute
   * @param from
   *          where to start
   * @param size
   *          what size to request
   * @return
   */
  public List<Map<String, Object>> query(String uri, long from, int size) throws IOException {
    String q = URIUtils.addParam(uri, "from=" + from, "size=" + size);

    Map hits = (Map) get(q, "hits");
    Object h = hits.get("hits");
    return (List<Map<String, Object>>) h;
  }

  private Object get(String q, String string) throws IOException {
    byte[] content = execute(new GetMethod(q));
    Map<String, Object> map = mapper.readValue(content, Map.class);
    return map.get(string);
  }

  public void addToIndex(String index, List<Object> values) throws IOException {
    // TODO: add bulk support
    for (Object object : values) {
      create(index, mapper.writeValueAsBytes(object));
    }
  }

  private void create(String q, byte[] value) throws IOException {
    PostMethod put = new PostMethod(q);
    put.setRequestEntity(new ByteArrayRequestEntity(value));
    execute(put);
  }

  public void deleteIndex(String index) throws IOException {
    execute(new DeleteMethod(index));
  }

  @Override
  public void close() {
    HttpConnectionManager manager = client.getHttpConnectionManager();
    if (manager instanceof SimpleHttpConnectionManager) {
      try {
        ((SimpleHttpConnectionManager) manager).closeIdleConnections(0);
      } catch (NullPointerException npe) {
        // ignore
      } catch (Exception ex) {
        // log - not much else to do
        log.warn("Exception closing underlying HTTP manager", ex);
      }
    }
  }

  private byte[] execute(HttpMethodBase method) throws IOException {
    try {
      int status = client.executeMethod(method);
      if (status >= 300) {
        String body;
        try {
          body = IOUtils.toString(method.getResponseBodyAsStream());
        } catch (IOException ex) {
          body = "";
        }

        throw new IOException(String.format("[%s] on [%s] failed; server returned [%s]", method.getName(),
            method.getURI(), body));
      }
      return method.getResponseBody();
    } finally {
      method.releaseConnection();
    }
  }
}