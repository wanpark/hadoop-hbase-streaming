/*
 * HBase OutputFormat for Hadoop Streaming.
 *
 * Format:
 * put	<RowID>	<ColumnName>	<value>	(<timestamp>)
 * delete	<RowID>	<ColumnName>	(<timestamp>)
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

import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hbase.io.BatchUpdate;

public class ListTableOutputFormat extends TextTableOutputFormat {

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

    public BatchUpdate[] createBatchUpdates(String command, String argsString) {
        String[] args = argsString.split(Pattern.quote(separator), -1);
        BatchUpdate bu = new BatchUpdate(args[0]);
        try {
            if (command.equals("put")) {
                put(bu, args);
            } else if (command.equals("delete")) {
                delete(bu, args);
            } else {
                throw new RuntimeException();
            }
        } catch(Exception e) {
            throw new RuntimeException(String.format("ListTableOutputFormat - invalid reduce output: %s / %s", command, argsString));
        }
        return new BatchUpdate[] { bu };
    }

    private void put(BatchUpdate bu, String[] args) {
        bu.put(decodeColumnName(args[1]), decodeValue(args[2]));
        if (args.length > 3) setTimestampString(bu, args[3]);
    }

    private void delete(BatchUpdate bu, String[] args) {
        bu.delete(decodeColumnName(args[1]));
        if (args.length > 2) setTimestampString(bu, args[2]);
    }

    private void setTimestampString(BatchUpdate bu, String ts) {
        try {
            bu.setTimestamp(Long.parseLong(ts));
        } catch(NumberFormatException e) {}
    }

}
