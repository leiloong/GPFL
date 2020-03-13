package ac.uk.ncl.utils;

import ac.uk.ncl.Settings;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.text.MessageFormat;

public class Helpers {
    public static double timerAndMemory(long s, String m, DecimalFormat f, Runtime r) {
        String time = f.format((double) (System.currentTimeMillis() - s) / 1000);
        String memory = f.format(((double) r.totalMemory() - r.freeMemory()) / (1024L * 1024L));
        Logger.println(MessageFormat.format("{0}: time = {1}s | memory = {2}mb", m, time, memory), 2);
        return Double.parseDouble(memory);
    }

    public static void timer(long s, String m) {
        DecimalFormat f = new DecimalFormat("###.###");
        String time = f.format((double) (System.currentTimeMillis() - s) / 1000);
        System.out.println(MessageFormat.format("{0}: time = {1}s", m, time));
    }

    public static JSONObject buildJSONObject(File file) {
        JSONObject args = null;

        try (InputStream in = new FileInputStream(file)) {
            JSONTokener tokener = new JSONTokener(in);
            args = new JSONObject(tokener);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return args;
    }

    public static void cleanDirectories(File root) {
        for (File cv : root.listFiles()) {
            for (File data : cv.listFiles()) {
                data.delete();
            }
            cv.delete();
        }
    }

    public static double formatDouble(DecimalFormat format, double value) {
        return Double.parseDouble(format.format(value));
    }

    public static boolean readSetting(JSONObject args, String key, boolean defaultValue) {
        if(args.has(key)) return args.getBoolean(key);
        else return defaultValue;
    }

    public static int readSetting(JSONObject args, String key, int defaultValue) {
        if(args.has(key)) return args.getInt(key);
        else return defaultValue;
    }

    public static double readSetting(JSONObject args, String key, double defaultValue) {
        if(args.has(key)) return args.getDouble(key);
        else return defaultValue;
    }

    public static String readSetting(JSONObject args, String key, String defaultValue) {
        if(args.has(key)) return args.getString(key);
        else return defaultValue;
    }

    public static long JVMRam() {
        long ram = Runtime.getRuntime().maxMemory();
        return (long) Math.ceil(ram / (1024d * 1024 * 1024));
    }

    public static long systemRAM() {
        long memorySize = ((com.sun.management.OperatingSystemMXBean) ManagementFactory
                .getOperatingSystemMXBean()).getTotalPhysicalMemorySize();
        return (long) Math.ceil(memorySize / (1024d * 1024 * 1024));
    }

    public static void reportSettings() {
        String msg = MessageFormat.format("\n# Settings:\n" +
                "# Depth = {0} | " +
                "Standard Confidence = {1}\n" +
                "# Support = {2} | " +
                "Top Instantiated Rules = {3}\n" +
                "# Top Abstract Rules = {4} | " +
                "Head Cap = {5}\n" +
                "# Learn Groundings = {6} | " +
                "Apply Groundings = {7}\n" +
                "# Saturation = {8} | " +
                "Batch Size = {9}\n" +
                "# Split Ratio = {10} | Neo4J Identifier = {11}\n" +
                "# Instantiated Rule Cap = {12} | Confidence Offset = {13}\n" +
                "# Suggestion Cap = {14} | Tail Cap = {15}"
                , Settings.DEPTH
                , String.valueOf(Settings.STANDARD_CONF)
                , Settings.SUPPORT
                , Settings.TOP_INS_RULES
                , Settings.TOP_ABS_RULES
                , Settings.HEAD_CAP
                , Settings.LEARN_GROUNDINGS
                , Settings.APPLY_GROUNDINGS
                , Settings.SATURATION
                , Settings.BATCH_SIZE
                , Settings.SPLIT_RATIO
                , Settings.NEO4J_IDENTIFIER
                , Settings.INS_RULE_CAP
                , Settings.CONFIDENCE_OFFSET
                , Settings.SUGGESTION_CAP
                , Settings.TAIL_CAP);
        Logger.println(msg, 1);
    }
}


