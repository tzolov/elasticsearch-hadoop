package org.elasticsearch.hadoop.integration.crunch.writable.e2e;

import org.elasticsearch.hadoop.integration.crunch.writable.CrunchMultipleInputsTest;
import org.elasticsearch.hadoop.integration.crunch.writable.CrunchMultipleOutputsTest;
import org.elasticsearch.hadoop.integration.crunch.writable.CrunchReadWriteTest;
import org.elasticsearch.hadoop.integration.crunch.writable.e2e.CrunchMapSerDeIT;
import org.elasticsearch.hadoop.integration.crunch.writable.e2e.CrunchWritableSerDeIT;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ CrunchMapSerDeIT.class, CrunchWritableSerDeIT.class,
    CrunchMultipleInputsTest.class, CrunchMultipleOutputsTest.class, CrunchReadWriteTest.class})
public class CrunchWritableEndToEndSuite {

}
