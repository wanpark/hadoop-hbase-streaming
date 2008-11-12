package org.childtv.hadoop.hbase.mapred;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.mapred.TableInputFormat;

public class ListTableInputFormat extends TextTableInputFormat {

    public static final String VALUE_SEPARATOR_KEY = "map.input.value.separator";
    public static final String DEFAULT_VALUE_SEPARATOR = " ";

    private String valueSeparator;

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        valueSeparator = job.get(VALUE_SEPARATOR_KEY);
        if (valueSeparator == null)
            valueSeparator = DEFAULT_VALUE_SEPARATOR;
    }

    public String getValueSeparator() { return valueSeparator; }


    public String formatRowResult(RowResult row) {
        StringBuilder values = new StringBuilder("");
        for (Cell cell : row.values()) {
            if (values.length() != 0)
                values.append(getValueSeparator());
            values.append(encodeValue(cell.getValue()));
        }
        return values.toString();
    }

}
