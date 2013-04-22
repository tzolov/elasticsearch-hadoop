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
package org.elasticsearch.hadoop.unit.util.curnch;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.elasticsearch.hadoop.crunch.ESTarget;
import org.junit.Test;

public class ESTargetTest {

  @Test
  public void testTargetEquality() {
    ESTarget s1 = new ESTarget.Builder("index").setHost("hostName").setPort(666).build();

    assertEquals(s1, s1);
    assertFalse(s1.equals(null));
    assertFalse(s1.equals("Different class"));

    assertEquals(s1, new ESTarget.Builder("index").setHost("hostName").setPort(666).build());
    assertFalse("Distinct index", s1.equals(new ESTarget.Builder("index2").setHost("hostName").setPort(666).build()));
    assertFalse("Different host", s1.equals(new ESTarget.Builder("index").setHost("hostName2").setPort(666).build()));
    assertFalse("Different port",
        s1.equals(new ESTarget.Builder("index").setHost("hostName").setPort(666 + 666).build()));

    assertEquals(new ESTarget.Builder("index").build(), new ESTarget.Builder("index").build());
    assertFalse(new ESTarget.Builder("index").build().equals(new ESTarget.Builder("index2").build()));
  }
}
