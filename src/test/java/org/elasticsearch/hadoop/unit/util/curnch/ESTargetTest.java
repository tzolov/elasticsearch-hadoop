package org.elasticsearch.hadoop.unit.util.curnch;

import static junit.framework.Assert.*;

import org.elasticsearch.hadoop.crunch.ESTarget;
import org.elasticsearch.hadoop.crunch.ESTarget.Builder;
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
