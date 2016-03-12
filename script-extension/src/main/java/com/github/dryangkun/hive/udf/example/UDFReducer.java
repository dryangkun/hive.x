package com.github.dryangkun.hive.udf.example;

import com.github.dryangkun.hive.HxContext;
import com.github.dryangkun.hive.HxLazyStruct;
import com.github.dryangkun.hive.HxOutput;
import com.github.dryangkun.hive.udf.HxUDFReducer;

import java.util.HashSet;
import java.util.Set;

// (a string, b string) -> (a string, x int, y date)
public class UDFReducer implements HxUDFReducer.UDF {

    private Set<String> set;

    @Override
    public void setup(HxContext context) throws Exception {
        set = new HashSet<>();
    }

    @Override
    public void reduceBegin(Object[] keys, HxContext context) throws Exception {
        set.clear();
    }

    @Override
    public void reduce(Object[] keys, HxLazyStruct row, HxContext context) throws Exception {
        set.add(row.getString(1));
    }

    @Override
    public void reduceEnd(Object[] keys, HxContext context) throws Exception {
        HxOutput output = context.getOutput();
        output.reset();

        output.write(0, keys[0]);
        output.write(1, set.size());
        output.write(2, context.createDate(System.currentTimeMillis()));
        set.clear();

        context.forward();
    }

    @Override
    public void cleanup(HxContext context) throws Exception {

    }
}
