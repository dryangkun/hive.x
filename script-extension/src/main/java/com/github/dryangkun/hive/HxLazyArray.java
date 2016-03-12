package com.github.dryangkun.hive;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.List;

public class HxLazyArray {

    private final ListObjectInspector oi;
    private Object data;

    public HxLazyArray(ListObjectInspector oi, Object data) {
        this.oi = oi;
        this.data = data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public ObjectInspector getElementOI() {
        return oi.getListElementObjectInspector();
    }

    public int getLength() {
        return oi.getListLength(data);
    }

    public Object getElement(int index) {
        return oi.getListElement(data, index);
    }

    @SuppressWarnings("unchecked")
    public List<Object> toList() {
        return (List<Object>) HxContext.toObject(data, oi);
    }
}
