package com.github.dryangkun.hive;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class HxScriptExtensionFactoryTest {

    public static void main(String[] args) throws Exception {
        (new HxScriptExtensionFactoryTest()).createExtension();
    }

    @Test public void createExtension() throws Exception {
        HxScriptExtensionFactory factory = new HxScriptExtensionFactory();

        Configuration conf = new Configuration();
        conf.set(HxScriptExtensionFactory.EXTESION_EXTRA_CLASSES, "");
        factory.initialize(conf);

        Assert.assertNotNull(factory.createExtension("#mapudf"));
        Assert.assertNotNull(factory.createExtension("#redudf"));
        Assert.assertNotNull(factory.createExtension("#mapjs"));
        Assert.assertNotNull(factory.createExtension("#redjs"));
    }
}
