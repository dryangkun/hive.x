package com.github.dryangkun.hive.nashorn;

import jdk.nashorn.api.scripting.ClassFilter;

public class HxNoThreadClassFilter implements ClassFilter {

    @Override
    public boolean exposeToScripts(String s) {
        if (s != null) {
            if ("java.lang.Thread".equals(s)) {
                return false;
            } else if (s.startsWith("java.util.concurrent")) {
                return false;
            }
        }
        return true;
    }
}
