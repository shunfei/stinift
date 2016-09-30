package com.sf.stinift.utils;

import com.sf.stinift.exchange.Fetchable;
import com.sf.stinift.exchange.Row;

import org.apache.commons.io.Charsets;

import java.io.IOException;
import java.io.InputStream;

public class RowInputStream extends InputStream {
    private final Fetchable fetchable;

    private static final char fieldSep = '\t';
    private static final char lineSep = '\n';
    private static final String nullField = "\\N";

    private int position = 0;
    private byte[] buffer;
    private StringBuilder stringBuilder = new StringBuilder(1 << 20);

    public RowInputStream(Fetchable fetchable) {
        this.fetchable = fetchable;
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int count = read(b, 0, 1);
        if (count == -1) {
            return -1;
        }
        return b[0];
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        if (buffer == null || position == buffer.length) {
            if (!fillBuffer()) {
                return -1;
            }
        }
        len = Math.min(buffer.length - position, len);
        System.arraycopy(buffer, position, b, off, len);
        position += len;
        return len;
    }

    private boolean fillBuffer() {
        Row row = (Row) fetchable.fetch();
        if (row == null) {
            return false;
        }
        buffer = toCSV(row).getBytes(Charsets.UTF_8);
        position = 0;
        return true;
    }

    private String toCSV(Row row) {
        stringBuilder.setLength(0);
        String field;
        for (int i = 0, count = row.fieldCount(); i < count; i++) {
            field = row.getField(i);
            if (null == field) {
                stringBuilder.append(nullField);
            } else {
                field = field.replace("\\", "\\\\");
                stringBuilder.append(field);
            }
            if (i < count - 1) {
                stringBuilder.append(fieldSep);
            } else {
                stringBuilder.append(lineSep);
            }
        }
        return stringBuilder.toString();
    }

}
