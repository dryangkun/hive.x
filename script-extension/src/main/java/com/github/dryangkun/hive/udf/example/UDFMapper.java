package com.github.dryangkun.hive.udf.example;

import com.github.dryangkun.hive.HxContext;
import com.github.dryangkun.hive.HxLazyStruct;
import com.github.dryangkun.hive.HxOutput;
import com.github.dryangkun.hive.udf.HxUDFMapper;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

// (a string, b string) ->
// (x array<string>, y int, z struct<z1:long,z2:double>)
public class UDFMapper implements HxUDFMapper.UDF {

    @Override
    public void setup(HxContext context) throws Exception {

    }

    @Override
    public void map(HxLazyStruct row, HxContext context) throws Exception {
        HxOutput output = context.getOutput();
        output.reset();

        String a = row.getString(0);
        String b = row.getString(1);

        List<String> x = new ArrayList<>();
        if (!StringUtils.isEmpty(a)) {
            for (int i = 0; i < a.length(); i++) {
                x.add("" + a.charAt(i));
            }
        }
        int y = b != null ? b.length() : 0;

        HxOutput z = output.getChildStruct(2);
        if (z != null) {
            z.reset();
            z.write(0, System.currentTimeMillis());
            z.write(1, 1.1d);
        }

        output.writeArray(0, x);
        output.write(1, y);
        output.writeStruct(2, z);

        context.forward();
        context.forward();
    }

    @Override
    public void cleanup(HxContext context) throws Exception {

    }
}
