package com.github.dryangkun.hive.nashorn;

import jdk.nashorn.api.scripting.ClassFilter;

public class HxAllClassFilter implements ClassFilter {

    @Override
    public boolean exposeToScripts(String s) {
        return true;
    }
}
