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
package org.elasticsearch.hadoop.integration.rest;

import java.util.Map;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.integration.TestSettings;
import org.elasticsearch.hadoop.rest.BufferedRestClient;
import org.elasticsearch.hadoop.rest.Node;
import org.elasticsearch.hadoop.rest.QueryBuilder;
import org.elasticsearch.hadoop.rest.ScrollQuery;
import org.elasticsearch.hadoop.rest.Shard;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 */
public class RestQueryTest {

    private BufferedRestClient client;
    private Settings settings;

    @Before
    public void start() {
        settings = new TestSettings("rest/savebulk");
        //settings.setPort(9200);
        client = new BufferedRestClient(settings);
    }

    @After
    public void stop() throws Exception {
        client.close();
    }

    @Test
    public void testShardInfo() throws Exception {
        Map<Shard, Node> shards = client.getTargetShards();
        System.out.println(shards);
        assertNotNull(shards);
    }

    @Test
    public void testQueryBuilder() throws Exception {
        QueryBuilder qb = QueryBuilder.query("rest/savebulk/_search?q=me*");

        int count = 0;
        for (ScrollQuery query = qb.build(client); query.hasNext();) {
            Map<String, Object> map = query.next();
            //System.out.println(map);
            assertNotNull(map);
            count++;
        }

        assertTrue(count > 0);
    }

    @Test
    public void testQueryShards() throws Exception {
        BufferedRestClient client = new BufferedRestClient(settings);
        Map<Shard, Node> targetShards = client.getTargetShards();

        String nodeId = targetShards.values().iterator().next().getId();
        ScrollQuery query = QueryBuilder.query("rest/savebulk/_search?q=me*")
                .shard("0")
                .onlyNode(nodeId)
                .time(settings.getScrollKeepAlive())
                .size(settings.getScrollSize())
                .build(client);

        int count = 0;
        for (; query.hasNext();) {
            Map<String, Object> map = query.next();
            //System.out.println(map);
            assertNotNull(map);
            count++;
        }

        assertTrue(count > 0);
    }
}
