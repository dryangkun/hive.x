package com.github.dryangkun.hive.nashorn;

import jdk.nashorn.api.scripting.ClassFilter;
import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class GetClassFilterTest {

    public static void main(String[] args) throws Exception {
        (new GetClassFilterTest()).getClassFilter1();

        System.out.println(ClassUtils.isAssignable(HxAllClassFilter.class, ClassFilter.class));
    }

    @Test public void getClassFilter1() throws Exception {
        Configuration conf = new Configuration();
        conf.set(HxNashornMapper.NASHORN_CLASSFILTER_CLASSES,
                "com.github.dryangkun.hive.nashorn.HxNoneClassFilter," +
                "com.github.dryangkun.hive.nashorn.HxNoThreadClassFilter," +
                "com.github.dryangkun.hive.nashorn.HxAllClassFilter");

        Assert.assertNull(HxNashornMapper.getClassFilter(conf));
    }

    @Test public void getClassFilter2() throws Exception {
        Configuration conf = new Configuration();
        conf.set(HxNashornMapper.NASHORN_CLASSFILTER_CLASSES,
                "com.github.dryangkun.hive.nashorn.HxNoneClassFilter");

        ClassFilter classFilter = HxNashornMapper.getClassFilter(conf);
        Assert.assertTrue(classFilter instanceof HxNoneClassFilter);
    }
}
