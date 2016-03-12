package com.github.dryangkun.hive.javascript;

import com.github.dryangkun.hive.HxAbstractScriptExtension;
import com.github.dryangkun.hive.HxExtensionDesc;
import com.github.dryangkun.hive.HxLazyStruct;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import javax.script.Invocable;
import javax.script.ScriptEngine;

// #mapjs js-file-or-resource [arg1 arg2 ...]
@HxExtensionDesc(prog = "mapjs")
public class HxJavaScriptMapper extends HxAbstractScriptExtension {

    public static final String SETUP_FUNC = "setup";
    public static final String MAP_FUNC = "map";
    public static final String CLEANUP_FUNC = "cleanup";

    private HxJavaScriptContext context;
    private Invocable invocable;
    private boolean hasCloseFunc;

    @Override
    public void initializeOp(ScriptOperator operator, String[] cmdArgs) throws Exception {
        super.initializeOp(operator, cmdArgs);

        if (cmdArgs.length < 2 || StringUtils.isEmpty(cmdArgs[1])) {
            throw new HiveException("cmdArgs[1] - js-file-or-resource can't be null");
        }
        String fileOrResource = cmdArgs[1];

        String[] args = createArgs(cmdArgs, 2);
        context = new HxJavaScriptContext(args, this);

        ScriptEngine scriptEngine = HxJavaScriptContext.createEngine();
        HxJavaScriptContext.load(scriptEngine, fileOrResource);

        if (scriptEngine.get(MAP_FUNC) == null) {
            throw new HiveException("map function not found in " + fileOrResource);
        }
        hasCloseFunc = scriptEngine.get(CLEANUP_FUNC) != null;

        invocable = (Invocable) scriptEngine;
        if (scriptEngine.get(SETUP_FUNC) != null) {
            invocable.invokeFunction(SETUP_FUNC, context);
        }
    }

    @Override
    protected void doProcess(HxLazyStruct struct) throws Exception {
        invocable.invokeFunction(MAP_FUNC, struct, context);
    }

    @Override
    public void close(boolean abort) throws Exception {
        if (hasCloseFunc) {
            invocable.invokeFunction(CLEANUP_FUNC, context);
        }
    }
}
