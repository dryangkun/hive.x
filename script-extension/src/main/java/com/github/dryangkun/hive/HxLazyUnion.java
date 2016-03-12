package com.github.dryangkun.hive;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;

public class HxLazyUnion {

    private final UnionObjectInspector oi;
    private Object data;

    public HxLazyUnion(UnionObjectInspector oi, Object data) {
        this.oi = oi;
        this.data = data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public byte getTag() {
        return oi.getTag(data);
    }

    public Object getField() {
        return oi.getField(data);
    }

    public ObjectInspector getFieldOI() {
        return oi.getObjectInspectors().get(getTag());
    }

    public Object toObject() {
        return HxContext.toObject(data, oi);
    }
}
