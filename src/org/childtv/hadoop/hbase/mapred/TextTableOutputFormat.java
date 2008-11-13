package org.childtv.hadoop.hbase.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Base64;

public abstract class TextTableOutputFormat
    implements OutputFormat<Text, Text>, JobConfigurable {

    public static final String TABLE_KEY = "reduce.output.table";
    public static final String IS_BINARY_KEY = "reduce.output.binary";

    protected TableOutputFormat outputFormat;
    private boolean isBinary;

    public TextTableOutputFormat() {
        outputFormat = new TableOutputFormat();
    }

    public void configure(JobConf job) {
        job.set(TableOutputFormat.OUTPUT_TABLE, job.get(TABLE_KEY));
        isBinary = argToBoolean(job.get(IS_BINARY_KEY));
    }

    public boolean isBinary() { return isBinary; }

    protected byte[] decodeColumnName(String name) {
        return isBinary() ? Base64.decode(name) : name.getBytes();
    }
    protected byte[] decodeValue(String value) {
        return isBinary() ? Base64.decode(value) : value.getBytes();
    }

    protected boolean argToBoolean(String arg) {
        if (arg == null) return false;
        return arg.equals("true")
            || arg.equals("yes")
            || arg.equals("on")
            || arg.equals("1");
    }

    public abstract BatchUpdate[] createBatchUpdates(String key, String content);

    public void checkOutputSpecs(FileSystem ignored, JobConf job)
        throws FileAlreadyExistsException, InvalidJobConfException, IOException {
        configure(job);
        outputFormat.checkOutputSpecs(ignored, job);
    }

    @SuppressWarnings("unchecked")
    public RecordWriter<Text, Text>  getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
        throws IOException {
        configure(job);
        return new TextTableRecordWriter(outputFormat.getRecordWriter(ignored, job, name, progress));
    }

    protected class TextTableRecordWriter implements RecordWriter<Text, Text> {
        private RecordWriter<ImmutableBytesWritable, BatchUpdate> tableRecordWriter;

        public TextTableRecordWriter(RecordWriter<ImmutableBytesWritable, BatchUpdate> tableRecordWriter) {
            this.tableRecordWriter = tableRecordWriter;
        }

        public void close(Reporter reporter) throws IOException {
            tableRecordWriter.close(reporter);
        }

        public void write(Text key, Text value) throws IOException {
            for (BatchUpdate bu : createBatchUpdates(key.toString(), value.toString())) {
                tableRecordWriter.write(null, bu);
            }
        }
    }

}
