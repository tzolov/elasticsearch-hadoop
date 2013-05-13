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
package org.elasticsearch.hadoop.integration;

import java.io.File;
import java.util.Iterator;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;

import org.elasticsearch.hadoop.util.TestUtils;

import org.junit.rules.ExternalResource;

public class LocalES extends ExternalResource {

    private static ESEmbeddedServer es;
    public static final String CLUSTER_NAME = "ES-HADOOP-TEST";
    private static final String ES_DATA_PATH = "build/es.data";
    public static final String DATA_PORTS = "9700-9800";
    public static final String TRANSPORT_PORTS = "9800-9900";

    @Override
    protected void before() throws Throwable {
        TestUtils.hackHadoopStagingOnWin();

        if (es == null) {
            System.out.println("Starting Elasticsearch...");
            es = new ESEmbeddedServer(CLUSTER_NAME, ES_DATA_PATH, DATA_PORTS, TRANSPORT_PORTS);
            es.start();
        }
    }

    @Override
    protected void after() {
        if (es != null) {
            System.out.println("Stopping Elasticsearch...");
            es.stop();
            es = null;

            // delete data folder
            TestUtils.delete(new File(ES_DATA_PATH));
        }
    }
    
    public void refresIndex(String indexName) {
      new RefreshRequestBuilder(es.getClient().admin().indices()).setIndices(indexName).execute().actionGet();
    }

    public long countIndex(String indexName, String typeName) {
      return es.getClient().prepareCount(indexName).setTypes(typeName).execute().actionGet().getCount();
    }

    public Iterator<SearchHit> searchIndex(String indexName, String typeName) {
      return es.getClient().prepareSearch(indexName).setTypes(typeName).execute().actionGet().getHits().iterator();
    }

    public Client getClient() {
      return es.getClient();
    }
}
