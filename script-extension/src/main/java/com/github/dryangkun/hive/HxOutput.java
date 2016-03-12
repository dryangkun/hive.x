package com.github.dryangkun.hive;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;

import java.util.*;

public class HxOutput {

    public static HxOutput create(ObjectInspector oi) {
        return new HxOutput((StructObjectInspector) oi);
    }

    private final StructObjectInspector oi;
    private final List<? extends StructField> structFields;
    private final int size;
    private final Map<Integer, HxOutput> childrenStructs;

    private Object[] buffer;

    public HxOutput(StructObjectInspector oi) {
        this.oi = oi;
        List<? extends StructField> structFields = oi.getAllStructFieldRefs();
        if (!(structFields instanceof RandomAccess)) {
            structFields = new ArrayList<>(structFields);
        }
        this.structFields = structFields;
        this.size = structFields.size();

        childrenStructs = new HashMap<>();
        for (int i = 0; i < structFields.size(); i++) {
            StructField _structField = structFields.get(i);
            ObjectInspector _oi = _structField.getFieldObjectInspector();
            if (_oi.getCategory() == ObjectInspector.Category.STRUCT) {
                childrenStructs.put(i, create(_oi));
            }
        }
    }

    public StructObjectInspector getOutputObjInspector() {
        return oi;
    }

    public HxOutput getChildStruct(int index) {
        return childrenStructs.get(index);
    }

    public ObjectInspector getFieldOI(int index) {
        return structFields.get(index).getFieldObjectInspector();
    }

    public void reset() {
        buffer = new Object[structFields.size()];
    }

    protected Object[] getBuffer() {
        return buffer;
    }

    final public void write(int index, Object data) {
        if (index < size) {
            buffer[index] = data;
        }
    }

    public void writeBinary(int index, BytesWritable data) {
        if (data != null) {
            write(index, data.copyBytes());
        }
    }

    public <T> void writeArray(int index, List<T> data) {
        write(index, data);
    }

    public <T> void writeArray(int index, T[] data) {
        if (data != null) {
            write(index, Arrays.asList(data));
        }
    }

    public <K,V> void writeMap(int index, Map<K,V> data) {
        write(index, data);
    }

    public void writeUnion(int index, byte tag, Object data) {
        write(index, new StandardUnionObjectInspector.StandardUnion(tag, data));
    }

    public void writeStruct(int index, HxOutput struct) {
        if (struct != null) {
            write(index, struct.getBuffer());
        }
    }
}
