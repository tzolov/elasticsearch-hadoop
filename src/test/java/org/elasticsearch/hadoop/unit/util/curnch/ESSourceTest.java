package org.elasticsearch.hadoop.unit.util.curnch;

import static junit.framework.Assert.*;

import org.elasticsearch.hadoop.crunch.ESSource;
import org.elasticsearch.hadoop.crunch.ESSource.Builder;
import org.junit.Test;

public class ESSourceTest {

  @Test
  public void testSourceEquality() {
    ESSource s1 = new ESSource.Builder("query").setHost("hostName").setPort(666).build();

    assertEquals(s1, s1);
    assertFalse(s1.equals(null));
    assertFalse(s1.equals("Different class"));

    assertEquals(s1, new ESSource.Builder("query").setHost("hostName").setPort(666).build());
    assertFalse("Distinct query", s1.equals(new ESSource.Builder("query2").setHost("hostName").setPort(666).build()));
    assertFalse("Different host", s1.equals(new ESSource.Builder("query").setHost("hostName2").setPort(666).build()));
    assertFalse("Different port",
        s1.equals(new ESSource.Builder("query").setHost("hostName").setPort(666 + 666).build()));

    assertEquals(new ESSource.Builder("query").build(), new ESSource.Builder("query").build());
    assertFalse(new ESSource.Builder("query").build().equals(new ESSource.Builder("query2").build()));
  }
}
