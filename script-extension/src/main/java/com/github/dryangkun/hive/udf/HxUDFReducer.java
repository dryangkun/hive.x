package com.github.dryangkun.hive.udf;

import com.github.dryangkun.hive.HxAbstractReducer;
import com.github.dryangkun.hive.HxContext;
import com.github.dryangkun.hive.HxExtensionDesc;
import com.github.dryangkun.hive.HxLazyStruct;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

// #redudf key-index1[,key-index2,...] classname [arg1 arg2 ...]
@HxExtensionDesc(prog = "redudf")
public class HxUDFReducer extends HxAbstractReducer {

    public interface UDF {
        void setup(HxContext context) throws Exception;
        void reduceBegin(Object[] keys, HxContext context) throws Exception;
        void reduce(Object[] keys, HxLazyStruct row, HxContext context) throws Exception;
        void reduceEnd(Object[] keys, HxContext context) throws Exception;
        void cleanup(HxContext context) throws Exception;
    }

    private HxContext context;
    private UDF udf;

    @Override
    public void initializeOp(ScriptOperator operator, String[] cmdArgs) throws Exception {
        super.initializeOp(operator, cmdArgs);

        if (cmdArgs.length < 3 || StringUtils.isEmpty(cmdArgs[2])) {
            throw new HiveException("cmdArgs[3] - classname can't be null");
        }
        String udfClass = cmdArgs[2];
        udf = (UDF) Class.forName(udfClass).newInstance();

        String[] args = createArgs(cmdArgs, 3);
        context = new HxContext(args, this);
        udf.setup(context);
    }

    @Override
    protected void reduceBegin(Object[] keys) throws Exception {
        udf.reduceBegin(keys, context);
    }

    @Override
    protected void reduce(Object[] keys, HxLazyStruct struct) throws Exception {
        udf.reduce(keys, struct, context);
    }

    @Override
    protected void reduceEnd(Object[] keys) throws Exception {
        udf.reduceEnd(keys, context);
    }

    @Override
    protected void doClose(boolean abort) throws Exception {
        udf.cleanup(context);
    }
}
