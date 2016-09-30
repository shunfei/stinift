package com.sf.stinift.exchange;

import org.apache.commons.lang3.StringUtils;

/**
 * Row is a common form of bee,
 */
public class Row implements Bee {
    private String[] rowVals;

    public Row(int fieldNum) {
        this.rowVals = new String[fieldNum];
    }

    public void setField(int index, String value) {
        rowVals[index] = value;
    }

    public String getField(int index) {
        if (index >= rowVals.length) {
            return null;
        } else {
            return rowVals[index];
        }
    }

    public int fieldCount() {
        return rowVals.length;
    }

    public String joinVals(String sep) {
        return StringUtils.join(rowVals, sep);
    }

    @Override
    public String toString() {
        return joinVals(",");
    }
}
