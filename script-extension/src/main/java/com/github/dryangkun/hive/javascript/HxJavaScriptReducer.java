package com.github.dryangkun.hive.javascript;

import com.github.dryangkun.hive.HxAbstractReducer;
import com.github.dryangkun.hive.HxExtensionDesc;
import com.github.dryangkun.hive.HxLazyStruct;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import javax.script.Invocable;
import javax.script.ScriptEngine;

// #redjs key-index1[,key-index2...] js-file-or-resource [arg1 arg2 ...]
@HxExtensionDesc(prog = "redjs")
public class HxJavaScriptReducer extends HxAbstractReducer {

    public static final String SETUP_FUNC = "setup";
    public static final String REDUCE_BEGIN_FUNC = "reduce_begin";
    public static final String REDUCE_FUNC = "reduce";
    public static final String REDUCE_END_FUNC = "reduce_end";
    public static final String CLEANUP_FUNC = "cleanup";

    private HxJavaScriptContext context;
    private Invocable invocable;
    private boolean hasCloseFunc;

    @Override
    public void initializeOp(ScriptOperator operator, String[] cmdArgs) throws Exception {
        super.initializeOp(operator, cmdArgs);

        if (cmdArgs.length < 3 || StringUtils.isEmpty(cmdArgs[2])) {
            throw new HiveException("cmdArgs[2] - js-file-or-resource can't be null");
        }
        String fileOrResource = cmdArgs[2];

        String[] args = createArgs(cmdArgs, 3);
        context = new HxJavaScriptContext(args, this);

        ScriptEngine scriptEngine = HxJavaScriptContext.createEngine();
        HxJavaScriptContext.load(scriptEngine, fileOrResource);

        if (scriptEngine.get(REDUCE_BEGIN_FUNC) == null ||
            scriptEngine.get(REDUCE_FUNC) == null ||
            scriptEngine.get(REDUCE_END_FUNC) == null) {
            throw new HiveException("reduce_begin/reduce/reduce_end function not found in " + fileOrResource);
        }
        hasCloseFunc = scriptEngine.get(CLEANUP_FUNC) != null;

        invocable = (Invocable) scriptEngine;
        if (scriptEngine.get(SETUP_FUNC) != null) {
            invocable.invokeFunction(SETUP_FUNC, context);
        }
    }

    @Override
    protected void reduceBegin(Object[] keys) throws Exception {
        invocable.invokeFunction(REDUCE_BEGIN_FUNC, keys, context);
    }

    @Override
    protected void reduce(Object[] keys, HxLazyStruct struct) throws Exception {
        invocable.invokeFunction(REDUCE_FUNC, keys, struct, context);
    }

    @Override
    protected void reduceEnd(Object[] keys) throws Exception {
        invocable.invokeFunction(REDUCE_END_FUNC, keys, context);
    }

    @Override
    protected void doClose(boolean abort) throws Exception {
        if (hasCloseFunc) {
            invocable.invokeFunction(CLEANUP_FUNC);
        }
    }
}
