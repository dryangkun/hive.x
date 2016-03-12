package com.github.dryangkun.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.Arrays;

// #redxxx key-index1[,key-index2...] ...
public abstract class HxAbstractReducer extends HxAbstractScriptExtension {

    protected int[] keyIndexes;
    protected Object[] bufferKeys;
    protected Object[] lastKeys;

    @Override
    public void initializeOp(ScriptOperator operator, String[] cmdArgs) throws Exception {
        super.initializeOp(operator, cmdArgs);

        if (cmdArgs.length < 2 || StringUtils.isEmpty(cmdArgs[1])) {
            throw new HiveException("cmdArgs[1] - key-indexes can't be null");
        }
        String[] keyIndexStrs = cmdArgs[1].split(",");
        keyIndexes = new int[keyIndexStrs.length];
        for (int i = 0; i < keyIndexStrs.length; i++) {
            String str = StringUtils.deleteWhitespace(keyIndexStrs[i]);
            int keyIndex = Integer.parseInt(str);
            keyIndexes[i] = keyIndex;
        }
        bufferKeys = new Object[keyIndexes.length];
    }

    @Override
    protected void doProcess(HxLazyStruct struct) throws Exception {
        for (int i = 0; i < keyIndexes.length; i++) {
            bufferKeys[i] = struct.getObject(keyIndexes[i]);
        }

        if (lastKeys == null) {
            lastKeys = new Object[keyIndexes.length];
            System.arraycopy(bufferKeys, 0, lastKeys, 0, lastKeys.length);

            reduceBegin(lastKeys);
            reduce(lastKeys, struct);
        } else {
            if (Arrays.equals(bufferKeys, lastKeys)) {
                reduce(lastKeys, struct);
            } else {
                reduceEnd(lastKeys);

                System.arraycopy(bufferKeys, 0, lastKeys, 0, lastKeys.length);
                reduceBegin(lastKeys);
                reduce(lastKeys, struct);
            }
        }
    }

    @Override
    public void close(boolean abort) throws Exception {
        if (lastKeys != null) {
            reduceEnd(lastKeys);
            lastKeys = null;
        }
        doClose(abort);
    }

    protected abstract void reduceBegin(Object[] keys) throws Exception;

    protected abstract void reduce(Object[] keys, HxLazyStruct struct) throws Exception;

    protected abstract void reduceEnd(Object[] keys) throws Exception;

    protected abstract void doClose(boolean abort) throws Exception;
}
