package com.github.dryangkun.hive.udf;

import com.github.dryangkun.hive.HxAbstractScriptExtension;
import com.github.dryangkun.hive.HxContext;
import com.github.dryangkun.hive.HxExtensionDesc;
import com.github.dryangkun.hive.HxLazyStruct;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

// #mapudf classname [arg1 arg2 ...]
@HxExtensionDesc(prog = "mapudf")
public class HxUDFMapper extends HxAbstractScriptExtension {

    public interface UDF {
        void setup(HxContext context) throws Exception;
        void map(HxLazyStruct row, HxContext context) throws Exception;
        void cleanup(HxContext context) throws Exception;
    }

    private HxContext context;
    private UDF udf;

    @Override
    public void initializeOp(ScriptOperator operator, String[] cmdArgs) throws Exception {
        super.initializeOp(operator, cmdArgs);

        if (cmdArgs.length < 2 || StringUtils.isEmpty(cmdArgs[1])) {
            throw new HiveException("cmdArgs[1] - classname can't be null");
        }
        String udfClass = cmdArgs[1];
        udf = (UDF) Class.forName(udfClass).newInstance();

        String[] args = createArgs(cmdArgs, 2);
        context = new HxContext(args, this);
        udf.setup(context);
    }

    @Override
    protected void doProcess(HxLazyStruct struct) throws Exception {
        udf.map(struct, context);
    }

    @Override
    public void close(boolean abort) throws Exception {
        udf.cleanup(context);
    }
}
