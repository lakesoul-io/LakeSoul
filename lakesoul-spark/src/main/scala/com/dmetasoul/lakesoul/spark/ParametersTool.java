// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.spark;

import org.apache.commons.lang3.math.NumberUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ParametersTool {

    protected final Map<String, String> data;
    protected static final String NO_VALUE_KEY = "__NO_VALUE_KEY";
    protected static final String DEFAULT_UNDEFINED = "<undefined>";
    protected transient Map<String, String> defaultData;
    protected transient Set<String> unrequestedParameters;

    public static ParametersTool fromArgs(String[] args) {
        Map<String, String> map = new HashMap(args.length / 2);
        int i = 0;

        while (i < args.length) {
            String key = getKeyFromArgs(args, i);
            if (key.isEmpty()) {
                throw new IllegalArgumentException("The input " + Arrays.toString(args) + " contains an empty argument");
            }

            ++i;
            if (i >= args.length) {
                map.put(key, "__NO_VALUE_KEY");
            } else if (NumberUtils.isNumber(args[i])) {
                map.put(key, args[i]);
                ++i;
            } else if (!args[i].startsWith("--") && !args[i].startsWith("-")) {
                map.put(key, args[i]);
                ++i;
            } else {
                map.put(key, "__NO_VALUE_KEY");
            }
        }
        return fromMap(map);
    }

    public static ParametersTool fromPropertiesFile(String path) throws IOException {
        File propertiesFile = new File(path);
        return fromPropertiesFile(propertiesFile);
    }

    public static ParametersTool fromPropertiesFile(File file) throws IOException {
        if (!file.exists()) {
            throw new FileNotFoundException("Properties file " + file.getAbsolutePath() + " does not exist");
        } else {
            FileInputStream fis = new FileInputStream(file);
            Throwable var2 = null;

            ParametersTool var3;
            try {
                var3 = fromPropertiesFile(fis);
            } catch (Throwable var12) {
                var2 = var12;
                throw var12;
            } finally {
                if (var2 != null) {
                    try {
                        fis.close();
                    } catch (Throwable var11) {
                        var2.addSuppressed(var11);
                    }
                } else {
                    fis.close();
                }
            }
            return var3;
        }
    }

    public static ParametersTool fromPropertiesFile(InputStream inputStream) throws IOException {
        Properties props = new Properties();
        props.load(inputStream);
        return fromMap(props);
    }

    public static ParametersTool fromMap(Map<String, String> map) {
        return new ParametersTool(map);
    }

    public static ParametersTool fromMap(Properties map) {
        return new ParametersTool(new HashMap(map));
    }

    public static ParametersTool fromSystemProperties() {
        return fromMap(System.getProperties());
    }

    private ParametersTool(Map<String, String> data) {
        this.data = Collections.unmodifiableMap(new HashMap(data));
        this.defaultData = new ConcurrentHashMap(data.size());
        this.unrequestedParameters = Collections.newSetFromMap(new ConcurrentHashMap(data.size()));
        this.unrequestedParameters.addAll(data.keySet());
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            ParametersTool that = (ParametersTool) o;
            return Objects.equals(this.data, that.data) && Objects.equals(this.defaultData, that.defaultData) && Objects.equals(this.unrequestedParameters, that.unrequestedParameters);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.data, this.defaultData, this.unrequestedParameters});
    }

    public int getNumberOfParameters() {
        return this.data.size();
    }

    public String get(String key) {
        this.addToDefaults(key, null);
        this.unrequestedParameters.remove(key);
        return this.data.get(key);
    }

    public boolean has(String value) {
        this.addToDefaults(value, null);
        this.unrequestedParameters.remove(value);
        return this.data.containsKey(value);
    }

//    public Configuration getConfiguration() {
//        Configuration conf = new Configuration();
//        Iterator var2 = this.data.entrySet().iterator();
//
//        while (var2.hasNext()) {
//            Map.Entry<String, String> entry = (Map.Entry) var2.next();
//            conf.setString((String) entry.getKey(), (String) entry.getValue());
//        }
//
//        return conf;
//    }

    public Properties getProperties() {
        Properties props = new Properties();
        props.putAll(this.data);
        return props;
    }

    public void createPropertiesFile(String pathToFile) throws IOException {
        this.createPropertiesFile(pathToFile, true);
    }

    public void createPropertiesFile(String pathToFile, boolean overwrite) throws IOException {
        File file = new File(pathToFile);
        if (file.exists()) {
            if (!overwrite) {
                throw new RuntimeException("File " + pathToFile + " exists and overwriting is not allowed");
            }

            file.delete();
        }

        Properties defaultProps = new Properties();
        defaultProps.putAll(this.defaultData);
        OutputStream out = new FileOutputStream(file);
        Throwable var6 = null;

        try {
            defaultProps.store(out, "Default file created by Flink's ParameterUtil.createPropertiesFile()");
        } catch (Throwable var15) {
            var6 = var15;
            throw var15;
        } finally {
            if (var6 != null) {
                try {
                    out.close();
                } catch (Throwable var14) {
                    var6.addSuppressed(var14);
                }
            } else {
                out.close();
            }
        }

    }

    protected Object clone() throws CloneNotSupportedException {
        return new ParametersTool(this.data);
    }

    public ParametersTool mergeWith(ParametersTool other) {
        Map<String, String> resultData = new HashMap(this.data.size() + other.data.size());
        resultData.putAll(this.data);
        resultData.putAll(other.data);
        ParametersTool ret = new ParametersTool(resultData);
        HashSet<String> requestedParametersLeft = new HashSet(this.data.keySet());
        requestedParametersLeft.removeAll(this.unrequestedParameters);
        HashSet<String> requestedParametersRight = new HashSet(other.data.keySet());
        requestedParametersRight.removeAll(other.unrequestedParameters);
        ret.unrequestedParameters.removeAll(requestedParametersLeft);
        ret.unrequestedParameters.removeAll(requestedParametersRight);
        return ret;
    }

    public Map<String, String> toMap() {
        return this.data;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.defaultData = new ConcurrentHashMap(this.data.size());
        this.unrequestedParameters = Collections.newSetFromMap(new ConcurrentHashMap(this.data.size()));
    }

    public static String getKeyFromArgs(String[] args, int index) {
        String key;
        if (args[index].startsWith("--")) {
            key = args[index].substring(2);
        } else {
            if (!args[index].startsWith("-")) {
                throw new IllegalArgumentException(String.format("Error parsing arguments '%s' on '%s'. Please prefix keys with -- or -.", Arrays.toString(args), args[index]));
            }

            key = args[index].substring(1);
        }

        if (key.isEmpty()) {
            throw new IllegalArgumentException("The input " + Arrays.toString(args) + " contains an empty argument");
        } else {
            return key;
        }
    }

    public String getRequired(String key) {
        this.addToDefaults(key, null);
        String value = this.get(key);
        if (value == null) {
            throw new RuntimeException("No data for required key '" + key + "'");
        } else {
            return value;
        }
    }

    public String get(String key, String defaultValue) {
        this.addToDefaults(key, defaultValue);
        String value = this.get(key);
        return value == null ? defaultValue : value;
    }

    public int getInt(String key) {
        this.addToDefaults(key, null);
        String value = this.getRequired(key);
        return Integer.parseInt(value);
    }

    public int getInt(String key, int defaultValue) {
        this.addToDefaults(key, Integer.toString(defaultValue));
        String value = this.get(key);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    public long getLong(String key) {
        this.addToDefaults(key, null);
        String value = this.getRequired(key);
        return Long.parseLong(value);
    }

    public long getLong(String key, long defaultValue) {
        this.addToDefaults(key, Long.toString(defaultValue));
        String value = this.get(key);
        return value == null ? defaultValue : Long.parseLong(value);
    }

    public float getFloat(String key) {
        this.addToDefaults(key, null);
        String value = this.getRequired(key);
        return Float.valueOf(value);
    }

    public float getFloat(String key, float defaultValue) {
        this.addToDefaults(key, Float.toString(defaultValue));
        String value = this.get(key);
        return value == null ? defaultValue : Float.valueOf(value);
    }

    public double getDouble(String key) {
        this.addToDefaults(key, null);
        String value = this.getRequired(key);
        return Double.valueOf(value);
    }

    public double getDouble(String key, double defaultValue) {
        this.addToDefaults(key, Double.toString(defaultValue));
        String value = this.get(key);
        return value == null ? defaultValue : Double.valueOf(value);
    }

    public boolean getBoolean(String key) {
        this.addToDefaults(key, null);
        String value = this.getRequired(key);
        return Boolean.valueOf(value);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        this.addToDefaults(key, Boolean.toString(defaultValue));
        String value = this.get(key);
        return value == null ? defaultValue : Boolean.valueOf(value);
    }

    public short getShort(String key) {
        this.addToDefaults(key, null);
        String value = this.getRequired(key);
        return Short.valueOf(value);
    }

    public short getShort(String key, short defaultValue) {
        this.addToDefaults(key, Short.toString(defaultValue));
        String value = this.get(key);
        return value == null ? defaultValue : Short.valueOf(value);
    }

    public byte getByte(String key) {
        this.addToDefaults(key, null);
        String value = this.getRequired(key);
        return Byte.valueOf(value);
    }

    public byte getByte(String key, byte defaultValue) {
        this.addToDefaults(key, Byte.toString(defaultValue));
        String value = this.get(key);
        return value == null ? defaultValue : Byte.valueOf(value);
    }

    protected void addToDefaults(String key, String value) {
        String currentValue = this.defaultData.get(key);
        if (currentValue == null) {
            if (value == null) {
                value = "<undefined>";
            }

            this.defaultData.put(key, value);
        } else if (currentValue.equals("<undefined>") && value != null) {
            this.defaultData.put(key, value);
        }

    }
}