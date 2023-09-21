// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.LakeSoulOptions;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.facebook.presto.common.type.*;
import com.facebook.presto.lakesoul.pojo.Path;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PrestoUtil {

    public static final String CDC_CHANGE_COLUMN = "lakesoul_cdc_change_column";
    public static final String CDC_CHANGE_COLUMN_DEFAULT = "rowKinds";

    private static final Pattern PARTITION_NAME_PATTERN = Pattern.compile("([^/]+)=([^/]+)");

    public static LinkedHashMap<String, String> extractPartitionSpecFromPath(Path path) {
        LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap();
        List<String[]> kvs = new ArrayList();
        org.apache.hadoop.fs.Path currPath = path;
        do {
            String component = currPath.getName();
            Matcher m = PARTITION_NAME_PATTERN.matcher(component);
            if (m.matches()) {
                String k = unescapePathName(m.group(1));
                String v = unescapePathName(m.group(2));
                String[] kv = new String[]{k, v};
                kvs.add(kv);
            }

            currPath = currPath.getParent();
        } while (currPath != null && !currPath.getName().isEmpty());

        for (int i = kvs.size(); i > 0; --i) {
            fullPartSpec.put(((String[]) kvs.get(i - 1))[0], ((String[]) kvs.get(i - 1))[1]);
        }

        return fullPartSpec;
    }

    public static String unescapePathName(String path) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < path.length(); ++i) {
            char c = path.charAt(i);
            if (c == '%' && i + 2 < path.length()) {
                int code = -1;

                try {
                    code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
                } catch (Exception var6) {
                }

                if (code >= 0) {
                    sb.append((char) code);
                    i += 2;
                    continue;
                }
            }

            sb.append(c);
        }

        return sb.toString();
    }

    public static boolean isExistHashPartition(TableInfo tif) {
        JSONObject tableProperties = JSON.parseObject(tif.getProperties());
        if (tableProperties.containsKey(LakeSoulOptions.HASH_BUCKET_NUM()) &&
                tableProperties.getString(LakeSoulOptions.HASH_BUCKET_NUM()).equals("-1")) {
            return false;
        } else {
            return tableProperties.containsKey(LakeSoulOptions.HASH_BUCKET_NUM());
        }
    }

    public static Map<String, Map<Integer, List<Path>>> splitDataInfosToRangeAndHashPartition(String tid,
                                                                                              DataFileInfo[] dfinfos) {
        Map<String, Map<Integer, List<Path>>> splitByRangeAndHashPartition = new LinkedHashMap<>();
        TableInfo tif = DataOperation.dbManager().getTableInfoByTableId(tid);
        for (DataFileInfo pif : dfinfos) {
            if (isExistHashPartition(tif) && pif.file_bucket_id() != -1) {
                splitByRangeAndHashPartition.computeIfAbsent(pif.range_partitions(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(pif.file_bucket_id(), v -> new ArrayList<>())
                        .add(new Path(pif.path()));
            } else {
                splitByRangeAndHashPartition.computeIfAbsent(pif.range_partitions(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(-1, v -> new ArrayList<>())
                        .add(new Path(pif.path()));
            }
        }
        return splitByRangeAndHashPartition;
    }

    /**
     * conver arrow type to presto type
     *
     * @param type arrow type
     * @return presto type
     */
    public static Type convertToPrestoType(ArrowType type) {
        return type.accept(ArrowTypeToLogicalTypeConverter.INSTANCE);
    }

    /**
     * a converter for arrow field
     */
    private static class ArrowTypeToLogicalTypeConverter
            implements ArrowType.ArrowTypeVisitor<Type> {

        private static final ArrowTypeToLogicalTypeConverter INSTANCE =
                new ArrowTypeToLogicalTypeConverter();

        @Override
        public Type visit(ArrowType.Null type) {
            return UnknownType.UNKNOWN;
        }

        @Override
        public Type visit(ArrowType.Struct type) {
            return UnknownType.UNKNOWN;
        }

        @Override
        public Type visit(ArrowType.List type) {
            return UnknownType.UNKNOWN;
        }

        @Override
        public Type visit(ArrowType.LargeList type) {
            return UnknownType.UNKNOWN;
        }

        @Override
        public Type visit(ArrowType.FixedSizeList type) {
            return UnknownType.UNKNOWN;
        }

        @Override
        public Type visit(ArrowType.Union type) {
            return UnknownType.UNKNOWN;
        }

        @Override
        public Type visit(ArrowType.Map type) {
            return UnknownType.UNKNOWN;
        }

        @Override
        public Type visit(ArrowType.Int type) {
            if (type.getBitWidth() == 64) {
                return BigintType.BIGINT;
            } else if (type.getBitWidth() == 32) {
                return IntegerType.INTEGER;
            } else if (type.getBitWidth() == 16) {
                return SmallintType.SMALLINT;
            } else if (type.getBitWidth() == 8) {
                return TinyintType.TINYINT;
            }
            return BigintType.BIGINT;
        }

        @Override
        public Type visit(ArrowType.FloatingPoint type) {
            if (type.getPrecision() == FloatingPointPrecision.HALF) {
                return UnknownType.UNKNOWN;
            } else if (type.getPrecision() == FloatingPointPrecision.SINGLE) {
                return RealType.REAL;
            } else if (type.getPrecision() == FloatingPointPrecision.DOUBLE) {
                return DoubleType.DOUBLE;
            }
            return DoubleType.DOUBLE;
        }

        @Override
        public Type visit(ArrowType.Utf8 type) {
            return VarcharType.VARCHAR;
        }

        @Override
        public Type visit(ArrowType.LargeUtf8 type) {
            return VarcharType.VARCHAR;
        }

        @Override
        public Type visit(ArrowType.Binary type) {
            return VarbinaryType.VARBINARY;
        }

        @Override
        public Type visit(ArrowType.LargeBinary type) {
            return VarbinaryType.VARBINARY;
        }

        @Override
        public Type visit(ArrowType.FixedSizeBinary type) {
            return VarbinaryType.VARBINARY;
        }

        @Override
        public Type visit(ArrowType.Bool type) {
            return BooleanType.BOOLEAN;
        }

        @Override
        public Type visit(ArrowType.Decimal type) {
            return DecimalType.createDecimalType(type.getPrecision(), type.getScale());
        }

        @Override
        public Type visit(ArrowType.Date type) {
            return DateType.DATE;
        }

        @Override
        public Type visit(ArrowType.Time type) {
            return TimeType.TIME;
        }

        @Override
        public Type visit(ArrowType.Timestamp type) {
            return TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
        }

        @Override
        public Type visit(ArrowType.Interval type) {
            return UnknownType.UNKNOWN;
        }

        @Override
        public Type visit(ArrowType.Duration type) {
            return UnknownType.UNKNOWN;
        }
    }


}
