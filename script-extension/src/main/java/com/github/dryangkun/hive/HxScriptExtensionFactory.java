package com.github.dryangkun.hive;

import org.apache.commons.lang.ClassUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.ql.exec.ScriptOperator.ScriptExtension;
import static org.apache.hadoop.hive.ql.exec.ScriptOperator.ScriptExtensionFactory;

public class HxScriptExtensionFactory implements ScriptExtensionFactory {

    private static final Log LOG = LogFactory.getLog(HxScriptExtensionFactory.class);

    public static final String HASH_TAG = "#";
    public static final String EXTESION_EXTRA_CLASSES = "hive.x.extension.extra.classes";

    private Map<String, Class<? extends ScriptExtension>> classes;

    @Override
    public void initialize(Configuration conf) {
        classes = getClasses(conf);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Class<? extends ScriptExtension>> getClasses(Configuration conf) {
        Map<String, Class<? extends ScriptExtension>> map = new HashMap<>();

        Class<? extends ScriptExtension> classes[] = new Class[] {
            com.github.dryangkun.hive.udf.HxUDFMapper.class,
            com.github.dryangkun.hive.udf.HxUDFReducer.class,
            com.github.dryangkun.hive.javascript.HxJavaScriptMapper.class,
            com.github.dryangkun.hive.javascript.HxJavaScriptReducer.class
        };
        for (Class<? extends ScriptExtension> clazz : classes) {
            Annotation desc = clazz.getAnnotation(HxExtensionDesc.class);
            LOG.debug("extension desc - " + desc);
            if (desc != null && (desc instanceof HxExtensionDesc)) {
                map.put(HASH_TAG + ((HxExtensionDesc) desc).prog().toLowerCase(), clazz);
            } else {
                LOG.error("don't get HxExtensionDesc annotation in class - " + clazz);
            }
        }

        try {
            Class<?> extraClasses[] = conf.getClasses(EXTESION_EXTRA_CLASSES);
            if (extraClasses != null) {
                for (Class clazz : extraClasses) {
                    LOG.debug("extra extension class - " + clazz);
                    if (ClassUtils.isAssignable(clazz, ScriptExtension.class)) {
                        Annotation desc = clazz.getAnnotation(HxExtensionDesc.class);
                        LOG.debug("extra extension desc - " + desc);
                        if (desc != null && (desc instanceof HxExtensionDesc)) {
                            map.put(HASH_TAG + ((HxExtensionDesc) desc).prog().toLowerCase(), clazz);
                        } else {
                            LOG.error("don't get HxExtensionDesc annotation in class - " + clazz);
                        }
                    } else {
                        LOG.error("class don't be subclass of ScriptExtension - " + clazz);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("get extension extra classes fail", e);
        }

        return map;
    }

    @Override
    public ScriptExtension createExtension(String prog) {
        ScriptExtension extension = null;
        if (prog != null) {
            prog = prog.toLowerCase();
            Class<? extends ScriptExtension> clazz = classes.get(prog);
            if (clazz != null) {
                try {
                    extension = (ScriptExtension) clazz.newInstance();
                } catch (Exception e) {
                    LOG.warn("new instance fail - " + clazz , e);
                }
            }
        }
        return extension;
    }
}
