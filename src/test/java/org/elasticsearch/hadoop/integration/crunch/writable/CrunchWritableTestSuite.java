package org.elasticsearch.hadoop.integration.crunch.writable;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ CrunchMultipleInputsTest.class, CrunchMultipleOutputsTest.class, CrunchReadWriteTest.class })
public class CrunchWritableTestSuite {

}
