package org.childtv.hadoop.hbase.streaming;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.streaming.StreamJob;

public class HadoopHBaseStreaming {

    public static void main(String[] args) throws IOException {
        Map<String, List<String>> options = parseArgs(args);

        String inputTable = deleteOption(options, "-inputtable");
        String inputColumn = deleteOption(options, "-inputcolumns");
        String inputFormat = deleteOption(options, "-inputformat");

        boolean isTableMapper = !inputTable.equals("");
        boolean mayExit = true;
        int returnStatus = 0;

        if (isTableMapper)
            addValue(options, "-input", inputTable);

        HBaseStreamJob job = new HBaseStreamJob(toArgStrings(options), mayExit);
        if (isTableMapper) {
            job.setInputTable(inputTable, inputColumn, inputFormat);
        }
        
        returnStatus = job.go();
        if (returnStatus != 0) {
            System.err.println("Streaming Job Failed!");
            System.exit(returnStatus);
        }
    }



    protected static Map<String, List<String>> parseArgs(String[] args) {
        Map<String, List<String>> options = new HashMap<String, List<String>>();
        String key = null;
        for (String arg : args) {
            if (arg.startsWith("-")) {
                if (key != null)
                    addValue(options, key, "");
                key = arg;
            } else {
                if (key == null) continue;
                addValue(options, key, arg);
                key = null;
            }
        }
        if (key != null)
            addValue(options, key, "");
        return options;
    }

    protected static <K, V> void addValue(Map<K, List<V>> map, K key, V value) {
        if (map.containsKey(key)) {
            map.get(key).add(value);
        } else {
            List<V> values = new ArrayList<V>();
            values.add(value);
            map.put(key, values);
        }
    }

    protected static String deleteOption(Map<String, List<String>> options, String key) {
        if (options.containsKey(key)) {
            String value = options.get(key).get(0);
            options.remove(key);
            return value;
        } else {
            return "";
        }
    }

    protected static String[] toArgStrings(Map<String, List<String>> options) {
        List<String> args = new ArrayList<String>();
        for (Map.Entry<String, List<String>> option : options.entrySet()) {
            String key = option.getKey();
            for (String value : option.getValue()) {
                args.add(key);
                if (!value.equals(""))
                    args.add(value);
            }
        }

        /*
        System.err.println("args: ");
        for (String arg: args) {
            System.err.println(arg);
        }
        */
        return args.toArray(new String[0]);
    }
}
