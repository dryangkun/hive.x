package com.github.dryangkun.hive.javascript;

import org.junit.Test;

import javax.script.ScriptEngine;

public class HxJavaScriptContextTest {

    @Test public void load() throws Exception {
        ScriptEngine engine = HxJavaScriptContext.createEngine();
        HxJavaScriptContext.load(engine, "[r]load_test.js");
    }
}
