package com.github.dryangkun.hive.javascript;

import com.github.dryangkun.hive.HxAbstractScriptExtension;
import com.github.dryangkun.hive.HxContext;
import com.github.dryangkun.hive.HxOutput;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.io.*;
import java.sql.Date;
import java.sql.Timestamp;

public class HxJavaScriptContext extends HxContext {

    public static final String RESOURCE_FLAG = "[r]";

    public HxJavaScriptContext(String[] args, HxAbstractScriptExtension extension) {
        super(args, extension);
    }

    @Override
    protected HxOutput createOutput(ObjectInspector oi) {
        return new HxJavaScriptOutput((StructObjectInspector) oi);
    }

    public static ScriptEngine createEngine() {
        return (new ScriptEngineManager()).getEngineByName("javascript");
    }

    public static Object load(ScriptEngine engine, String fileOrResurce) throws Exception {
        if (!fileOrResurce.startsWith(RESOURCE_FLAG)) {
            if (!fileOrResurce.startsWith("/") && !fileOrResurce.startsWith("./")) {
                fileOrResurce = "./" + fileOrResurce;
            }
            return loadFile(engine, fileOrResurce);
        } else {
            fileOrResurce = fileOrResurce.substring(3);
            return loadResource(engine, fileOrResurce);
        }
    }

    public static Object loadFile(ScriptEngine engine, String file) throws Exception {
        try (Reader reader = new FileReader(file)) {
            return engine.eval(reader);
        }
    }

    public static Object loadResource(ScriptEngine engine, String resource)  throws Exception {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
        if (in == null) {
            throw new IOException("resource - " + resource + " not found");
        }
        try (Reader reader = new InputStreamReader(in)) {
            return engine.eval(reader);
        }
    }

    public static Long toDateTime(Date date) {
        return date != null ? date.getTime() : null;
    }

    public static int[] toYearMonth(HiveIntervalYearMonth data) {
        if (data != null) {
            return new int[] {data.getYears(), data.getYears()};
        } else {
            return null;
        }
    }

    public static Long toTimestamp(Timestamp data) {
        return data != null ? data.getTime() : null;
    }
}
