package com.github.dryangkun.hive;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class HxScriptExtensionFactoryNashornTest {

    @Test public void createExtension() throws Exception {
        HxScriptExtensionFactory factory = new HxScriptExtensionFactory();

        Configuration conf = new Configuration();
        conf.set(HxScriptExtensionFactory.EXTESION_EXTRA_CLASSES,
                "com.github.dryangkun.hive.nashorn.HxNashornMapper," +
                "com.github.dryangkun.hive.nashorn.HxNashornReducer");
        factory.initialize(conf);

        Assert.assertNotNull(factory.createExtension("#mapnashorn"));
        Assert.assertNotNull(factory.createExtension("#rednashorn"));
    }
}
