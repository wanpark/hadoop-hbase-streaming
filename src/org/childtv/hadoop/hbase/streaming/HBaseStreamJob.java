package org.childtv.hadoop.hbase.streaming;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.streaming.StreamJob;
import org.apache.hadoop.streaming.StreamUtil;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.hbase.mapred.TableInputFormat;

import org.childtv.hadoop.hbase.mapred.JSONTableInputFormat;
import org.childtv.hadoop.hbase.mapred.XMLTableInputFormat;
import org.childtv.hadoop.hbase.mapred.ListTableInputFormat;

public class HBaseStreamJob extends StreamJob {

    private static Map<String, Class<? extends InputFormat>> inputFormatAbbr;
    static {
        inputFormatAbbr = new HashMap<String, Class<? extends InputFormat>>();
        inputFormatAbbr.put("json", JSONTableInputFormat.class);
        inputFormatAbbr.put("xml", XMLTableInputFormat.class);
        inputFormatAbbr.put("list", ListTableInputFormat.class);
    }

    private String table;
    private String columns;
    private Class<? extends InputFormat> inputFormat;

    public HBaseStreamJob(String[] args, boolean mayExit) {
        super(args, mayExit);
    }

    public void setInputTable(String table, String columns, String inputFormatName) {
        this.table = table;
        this.columns = columns;
        try {
            this.inputFormat = getInputFormatClassForName(inputFormatName);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("input format is not found: " + inputFormatName);
        }
    }

    @SuppressWarnings("unchecked")
    protected Class<? extends InputFormat> getInputFormatClassForName(String name) throws ClassNotFoundException {
        if (name == null || name.equals(""))
            return JSONTableInputFormat.class;

        if (inputFormatAbbr.containsKey(name))
            return inputFormatAbbr.get(name);

        for (Class<? extends InputFormat> clazz : inputFormatAbbr.values()) {
            if (name.equals(clazz.getName())
                || name.equals(clazz.getCanonicalName())
                || name.equals(clazz.getSimpleName())) {
                return clazz;
            }
        }

        Class clazz = StreamUtil.goodClassOrNull(name, this.getClass().getPackage().getName());
        if (clazz != null) {
            try {
                return (Class<? extends InputFormat>) clazz;
            } catch(Exception e) {
                throw new ClassNotFoundException();
            }
        }

        throw new ClassNotFoundException();
    }

    @Override
    protected void setJobConf() throws IOException {
        super.setJobConf();
        if (inputFormat != null) {
            jobConf_.setInputFormat(this.inputFormat);
            jobConf_.set(TableInputFormat.COLUMN_LIST, this.columns);
        }
    }
}
