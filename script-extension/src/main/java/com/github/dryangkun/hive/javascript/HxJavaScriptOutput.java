package com.github.dryangkun.hive.javascript;

import com.github.dryangkun.hive.HxOutput;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.sql.Date;

public class HxJavaScriptOutput extends HxOutput {

    public HxJavaScriptOutput(StructObjectInspector oi) {
        super(oi);
    }

    public void writeDate(int index, long time) {
        write(index, new Date(time));
    }

    public void writeByte(int index, Double data) {
        write(index, data != null ? data.byteValue() : null);
    }

    public void writeShort(int index, Double data) {
        write(index, data != null ? data.shortValue() : null);
    }

    public void writeInt(int index, Double data) {
        write(index, data != null ? data.intValue() : null);
    }

    public void writeLong(int index, Double data) {
        write(index, data != null ? data.longValue() : null);
    }

    public void writeFloat(int index, Double data) {
        write(index, data != null ? data.floatValue() : null);
    }
}
