package com.github.dryangkun.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public abstract class HxAbstractScriptExtension implements ScriptOperator.ScriptExtension {

    protected ScriptOperator operator;
    protected Configuration conf;
    protected Map<String, LongWritable> statsMap;

    protected ObjectInspector[] inputObjInspectors;
    protected ObjectInspector outputObjInspector;

    protected Map<Integer, HxLazyStruct> cacheStructs;

    public HxAbstractScriptExtension() {
    }

    @Override
    public void initializeOp(ScriptOperator operator, String[] cmdArgs) throws Exception {
        this.operator = operator;
        this.conf = operator.getConfigurationForExtension();
        this.statsMap = operator.getStatsMapForExtension();

        inputObjInspectors = operator.getInputObjInspectors();
        outputObjInspector = operator.getOutputObjInspector();
        if (!(outputObjInspector instanceof StructObjectInspector)) {
            throw new HiveException("ScriptOperator's OutputObjInspector is not " +
                    "StructObjectInspector - " + outputObjInspector.getClass());
        }
        cacheStructs = new HashMap<>(inputObjInspectors.length);
    }

    @Override
    public void process(Object row, int tag) throws Exception {
        HxLazyStruct struct = cacheStructs.get(tag);
        if (struct == null) {
            ObjectInspector oi = inputObjInspectors[tag];
            if (!(oi instanceof StructObjectInspector)) {
                throw new HiveException("InputObjInspectors[" + tag + "] not StructObjectInspector - "
                        + oi.getClass());
            }
            StructObjectInspector soi = (StructObjectInspector) oi;
            struct = new HxLazyStruct(soi, row);
            cacheStructs.put(tag, struct);
        } else {
            struct.setData(row);
        }
        doProcess(struct);
    }

    protected abstract void doProcess(HxLazyStruct struct) throws Exception;

    @Override
    public void close(boolean abort) throws Exception {

    }

    protected static String[] createArgs(String[] cmdArgs, int offset) {
        return cmdArgs.length > offset ? Arrays.copyOfRange(cmdArgs, offset, cmdArgs.length) : new String[0];
    }
}
