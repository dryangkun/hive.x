package com.github.dryangkun.hive.nashorn;

import jdk.nashorn.api.scripting.ClassFilter;

public class HxNoneClassFilter implements ClassFilter {

    @Override
    public boolean exposeToScripts(String s) {
        return false;
    }
}
