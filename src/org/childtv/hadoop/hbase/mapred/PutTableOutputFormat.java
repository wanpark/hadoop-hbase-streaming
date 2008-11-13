/*
 * HBase OutputFormat for Hadoop Streaming.
 * Only for insert data.
 *
 * Format:
 * <RowID>	<ColumnName>	<value>	(<timestamp>)
 *
 * Options:
 * -jobconf reduce.output.table=<TableName>
 * -jobconf reduce.output.binary=<true|false> (default: false)
 * -jobconf stream.reduce.output.field.separator=<Separator> (default: tab)
 *
 * Notice:
 * Run streaming with dammy -output option.
 */

package org.childtv.hadoop.hbase.mapred;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hbase.io.BatchUpdate;

public class PutTableOutputFormat extends TextTableOutputFormat {

    // defined at org.apache.hadoop.streaming.PipeMapRed
    public static final String SEPARATOR_KEY = "stream.reduce.output.field.separator";
    public static final String DEFAULT_SEPARATOR = "\t";

    private String separator;

    public void configure(JobConf job) {
        super.configure(job);
        separator = job.get(SEPARATOR_KEY);
        if (separator == null)
            separator = DEFAULT_SEPARATOR;
    }

    public BatchUpdate[] createBatchUpdates(String key, String content) {
        String[] values = content.split(separator, 3);
        if (values.length < 2)
            throw new RuntimeException("PutTableOutputFormat: invalid reduce output: " + content);

        
        BatchUpdate bu = new BatchUpdate(key);
        bu.put(decodeColumnName(values[0]), decodeValue(values[1]));
        if (values.length >= 3) {
            try {
                bu.setTimestamp(Long.parseLong(values[2]));
            } catch(NumberFormatException e) {}
        }
        
        return new BatchUpdate[] { bu };
    }

}
