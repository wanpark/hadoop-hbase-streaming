package org.childtv.hadoop.hbase.streaming;


import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.streaming.StreamJob;

public class HadoopHBaseStreaming {

    public static void main(String[] args) throws IOException {

        Map<String, String> options = parseArgs(args);

        String inputTable = deleteOption(options, "-inputtable");
        String inputColumn = deleteOption(options, "-inputcolumns");
        String inputFormat = deleteOption(options, "-inputformat");

        boolean mayExit = true;
        int returnStatus = 0;

        if (!inputTable.equals(""))
            options.put("-input", inputTable);

        HBaseStreamJob job = new HBaseStreamJob(toArgStrings(options), mayExit);
        if (!inputTable.equals("")) {
            job.setInputTable(inputTable, inputColumn, inputFormat);
        }
        
        returnStatus = job.go();
        if (returnStatus != 0) {
            System.err.println("Streaming Job Failed!");
            System.exit(returnStatus);
        }
    }


    protected static Map<String, String> parseArgs(String[] args) {
        Map<String, String> options = new HashMap<String, String>();
        String key = null;
        for (String arg : args) {
            if (arg.startsWith("-")) {
                if (key != null)
                    options.put(key, "");
                key = arg;
            } else {
                if (key != null) {
                    options.put(key, arg);
                    key = null;
                }
            }
        }
        if (key != null)
            options.put(key, "");
        return options;
    }

    protected static String deleteOption(Map<String, String> options, String key) {
        if (options.containsKey(key)) {
            String value = options.get(key);
            options.remove(key);
            return value;
        } else {
            return "";
        }
    }

    protected static String[] toArgStrings(Map<String, String> options) {
        List<String> args = new ArrayList<String>();
        for (Map.Entry<String, String> option : options.entrySet()) {
            args.add(option.getKey());
            if (!option.getValue().equals(""))
                args.add(option.getValue());
        }
        return args.toArray(new String[0]);
    }
}
