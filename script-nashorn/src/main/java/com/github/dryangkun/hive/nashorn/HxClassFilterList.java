package com.github.dryangkun.hive.nashorn;

import jdk.nashorn.api.scripting.ClassFilter;

import java.util.LinkedList;
import java.util.List;

public class HxClassFilterList implements ClassFilter {

    private List<ClassFilter> classFilters = new LinkedList<>();

    public void addClassFilter(ClassFilter classFilter) {
        classFilters.add(classFilter);
    }

    public List<ClassFilter> getClassFilters() {
        return classFilters;
    }

    @Override
    public boolean exposeToScripts(String s) {
        for (ClassFilter classFilter : classFilters) {
            if (classFilter.exposeToScripts(s)) {
                return true;
            }
        }
        return false;
    }
}
