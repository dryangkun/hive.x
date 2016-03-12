package com.github.dryangkun.hive;

import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.Map;

public class HxLazyMap {

    private final MapObjectInspector oi;
    private Object data;

    public HxLazyMap(MapObjectInspector oi, Object data) {
        this.oi = oi;
        this.data = data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public ObjectInspector getKeyOI() {
        return oi.getMapKeyObjectInspector();
    }

    public ObjectInspector getValueOI() {
        return oi.getMapValueObjectInspector();
    }

    public Map<?, ?> getMap() {
        return oi.getMap(data);
    }

    @SuppressWarnings("unchecked")
    public Map<Object, Object> toMap() {
        return (Map<Object, Object>) HxContext.toObject(data, oi);
    }
}
