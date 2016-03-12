package com.github.dryangkun.hive.nashorn;

import com.github.dryangkun.hive.HxAbstractScriptExtension;
import com.github.dryangkun.hive.HxContext;
import com.github.dryangkun.hive.HxExtensionDesc;
import com.github.dryangkun.hive.HxLazyStruct;
import com.github.dryangkun.hive.javascript.HxJavaScriptContext;
import com.github.dryangkun.hive.javascript.HxJavaScriptMapper;
import jdk.nashorn.api.scripting.ClassFilter;
import jdk.nashorn.api.scripting.NashornScriptEngine;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

@HxExtensionDesc(prog = "mapnashorn")
public class HxNashornMapper extends HxAbstractScriptExtension {

    public static final String NASHORN_CLASSFILTER_CLASSES = "hive.x.classfilter.classes";

    @SuppressWarnings("unchecked")
    public static ClassFilter getClassFilter(Configuration conf) throws Exception {
        Class<?>[] classes = conf.getClasses(NASHORN_CLASSFILTER_CLASSES, HxNoThreadClassFilter.class);

        HxClassFilterList classFilterList = new HxClassFilterList();
        for (Class clazz : classes) {
            HxContext.LOG.debug("nashorn class filter - " + clazz);
            if (ClassUtils.isAssignable(clazz, ClassFilter.class)) {
                try {
                    ClassFilter classFilter = (ClassFilter) clazz.newInstance();
                    if (classFilter instanceof HxAllClassFilter) {
                        return null;
                    }
                    classFilterList.addClassFilter(classFilter);
                } catch (Exception e) {
                    HxContext.LOG.error("nashorn new instance fail - " + clazz, e);
                }
            } else {
                HxContext.LOG.error("nashorn class don't be subclass of ClassFilter - " + clazz);
            }
        }

        if (classFilterList.getClassFilters().size() == 1) {
            return classFilterList.getClassFilters().get(0);
        } else {
            return classFilterList;
        }
    }

    public static ScriptObjectMirror getFunction(NashornScriptEngine engine, String name) throws Exception {
        Object obj = engine.get(name);
        if (obj != null) {
            if (obj instanceof ScriptObjectMirror) {
                ScriptObjectMirror mirror = (ScriptObjectMirror) obj;
                if (mirror.isFunction()) {
                    return mirror;
                }
            }
            throw new HiveException("name - " + name + " is not function - " + obj.getClass());
        }
        return null;
    }

    private HxJavaScriptContext context;
    private ScriptObjectMirror map;
    private ScriptObjectMirror cleanup;

    @Override
    public void initializeOp(ScriptOperator operator, String[] cmdArgs) throws Exception {
        super.initializeOp(operator, cmdArgs);

        if (cmdArgs.length < 2 || StringUtils.isEmpty(cmdArgs[1])) {
            throw new HiveException("cmdArgs[1] - js-file-or-resource can't be null");
        }
        String fileOrResource = cmdArgs[1];

        String[] args = createArgs(cmdArgs, 2);
        context = new HxJavaScriptContext(args, this);

        NashornScriptEngineFactory factory = new NashornScriptEngineFactory();
        ClassFilter classFilter = getClassFilter(conf);

        NashornScriptEngine engine = classFilter == null ?
                (NashornScriptEngine) factory.getScriptEngine() :
                (NashornScriptEngine) factory.getScriptEngine(classFilter);
        HxJavaScriptContext.load(engine, fileOrResource);

        map = getFunction(engine, HxJavaScriptMapper.MAP_FUNC);
        if (map == null) {
            throw new HiveException("not found map function in " + fileOrResource);
        }
        cleanup = getFunction(engine, HxJavaScriptMapper.CLEANUP_FUNC);

        ScriptObjectMirror setup = getFunction(engine, HxJavaScriptMapper.SETUP_FUNC);
        if (setup != null) {
            setup.call(null, context);
        }
    }

    @Override
    protected void doProcess(HxLazyStruct struct) throws Exception {
        map.call(null, struct, context);
    }

    @Override
    public void close(boolean abort) throws Exception {
        if (cleanup != null) {
            cleanup.call(null, context);
        }
    }
}
