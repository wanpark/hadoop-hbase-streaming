package org.childtv.hadoop.hbase.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.mapred.TableInputFormat;

import org.apache.noggit.JSONUtil;

public class JSONTableInputFormat extends TextTableInputFormat {

    public String formatRowResult(RowResult row) {
        return hasTimestamp() ? formatRowResultWithTimestamp(row) : formatRowResultWithoutTimestamp(row);
    }

    public String formatRowResultWithTimestamp(RowResult row) {
        Map<String, Map<String, String>> values = new HashMap<String, Map<String, String>>();
        for (Map.Entry<byte[], Cell> entry : row.entrySet()) {
            Map<String, String> cell = new HashMap<String, String>();
            cell.put("value", encodeValue(entry.getValue().getValue()));
            cell.put("timestamp", String.valueOf(entry.getValue().getTimestamp()));
            values.put(encodeKey(entry.getKey()), cell);
        }
        return JSONUtil.toJSON(values);
    }

    public String formatRowResultWithoutTimestamp(RowResult row) {
        Map<String, String> values = new HashMap<String, String>();
        for (Map.Entry<byte[], Cell> entry : row.entrySet()) {
            values.put(encodeKey(entry.getKey()), encodeValue(entry.getValue().getValue()));
        }
        return JSONUtil.toJSON(values);
    }

}
