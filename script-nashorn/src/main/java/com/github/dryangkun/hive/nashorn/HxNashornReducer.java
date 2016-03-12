package com.github.dryangkun.hive.nashorn;

import com.github.dryangkun.hive.HxAbstractReducer;
import com.github.dryangkun.hive.HxExtensionDesc;
import com.github.dryangkun.hive.HxLazyStruct;
import com.github.dryangkun.hive.javascript.HxJavaScriptContext;
import com.github.dryangkun.hive.javascript.HxJavaScriptReducer;
import jdk.nashorn.api.scripting.ClassFilter;
import jdk.nashorn.api.scripting.NashornScriptEngine;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

@HxExtensionDesc(prog = "rednashorn")
public class HxNashornReducer extends HxAbstractReducer {

    private HxJavaScriptContext context;
    private ScriptObjectMirror reduceBegin;
    private ScriptObjectMirror reduce;
    private ScriptObjectMirror reduceEnd;
    private ScriptObjectMirror cleanup;

    @Override
    public void initializeOp(ScriptOperator operator, String[] cmdArgs) throws Exception {
        super.initializeOp(operator, cmdArgs);

        if (cmdArgs.length < 3 || StringUtils.isEmpty(cmdArgs[2])) {
            throw new HiveException("cmdArgs[2] - js-file-or-resource can't be null");
        }
        String fileOrResource = cmdArgs[2];

        String[] args = createArgs(cmdArgs, 3);
        context = new HxJavaScriptContext(args, this);

        NashornScriptEngineFactory factory = new NashornScriptEngineFactory();
        ClassFilter classFilter = HxNashornMapper.getClassFilter(conf);

        NashornScriptEngine engine = classFilter == null ?
                (NashornScriptEngine) factory.getScriptEngine() :
                (NashornScriptEngine) factory.getScriptEngine(classFilter);
        HxJavaScriptContext.load(engine, fileOrResource);

        reduceBegin = HxNashornMapper.getFunction(engine, HxJavaScriptReducer.REDUCE_BEGIN_FUNC);
        reduce = HxNashornMapper.getFunction(engine, HxJavaScriptReducer.REDUCE_FUNC);
        reduceEnd = HxNashornMapper.getFunction(engine, HxJavaScriptReducer.REDUCE_END_FUNC);
        if (reduceBegin == null || reduce == null || reduceEnd == null) {
            throw new HiveException("reduce_begin/reduce/reduce_end function not found in " + fileOrResource);
        }

        cleanup = HxNashornMapper.getFunction(engine, HxJavaScriptReducer.CLEANUP_FUNC);
        ScriptObjectMirror setup = HxNashornMapper.getFunction(engine, HxJavaScriptReducer.SETUP_FUNC);
        if (setup != null) {
            setup.call(null, context);
        }
    }

    @Override
    protected void reduceBegin(Object[] keys) throws Exception {
        reduceBegin.call(null, keys, context);
    }

    @Override
    protected void reduce(Object[] keys, HxLazyStruct struct) throws Exception {
        reduce.call(null, keys, struct, context);
    }

    @Override
    protected void reduceEnd(Object[] keys) throws Exception {
        reduceEnd.call(null, keys, context);
    }

    @Override
    protected void doClose(boolean abort) throws Exception {
        if (cleanup != null) {
            cleanup.call(null, context);
        }
    }
}
