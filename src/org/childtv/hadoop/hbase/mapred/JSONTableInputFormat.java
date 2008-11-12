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
        Map<String, Map<String, String>> values = new HashMap<String, Map<String, String>>();
        for (Map.Entry<byte[], Cell> entry : row.entrySet()) {
            Map<String, String> cell = new HashMap<String, String>();
            cell.put("value", new String(entry.getValue().getValue()));
            cell.put("timestamp", String.valueOf(entry.getValue().getTimestamp()));
            values.put(new String(entry.getKey()), cell);
        }
        return JSONUtil.toJSON(values);
    }

}
