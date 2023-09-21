// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.dmetasoul.lakesoul.LakeSoulArrowReader;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import com.facebook.presto.common.type.DateTimeEncoding;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.lakesoul.handle.LakeSoulTableColumnHandle;
import com.facebook.presto.lakesoul.pojo.Path;
import com.facebook.presto.lakesoul.util.ArrowUtil;
import com.facebook.presto.lakesoul.util.PrestoUtil;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class LakeSoulRecordCursor implements RecordCursor {
    private final LakeSoulRecordSet recordSet;
    private final LakeSoulArrowReader reader;
    private int curRecordIdx = -1;
    private VectorSchemaRoot currentVCR;
    private List<Type> desiredTypes;
    LinkedHashMap<String, String> partitions;

    public LakeSoulRecordCursor(LakeSoulRecordSet recordSet) throws IOException {

        this.recordSet = recordSet;
        LakeSoulSplit split = this.recordSet.getSplit();
        NativeIOReader reader = new NativeIOReader();
        // set paths, schema, pks
        for (Path path : split.getPaths()) {
            reader.addFile(path.getFilename());
        }
        this.partitions = PrestoUtil.extractPartitionSpecFromPath(split.getPaths().get(0));

        List<Field> fields = recordSet.getColumnHandles().stream().map(item -> {
            LakeSoulTableColumnHandle columnHandle = (LakeSoulTableColumnHandle) item;
            return Field.nullable(columnHandle.getColumnName(),
                    ArrowUtil.convertToArrowType(columnHandle.getColumnType()));
        }).collect(Collectors.toList());
        HashMap<String, ColumnHandle> allcolumns = split.getLayout().getAllColumns();
        List<String> dataCols = recordSet.getColumnHandles().stream().map(item -> {
            LakeSoulTableColumnHandle columnHandle = (LakeSoulTableColumnHandle) item;
            return columnHandle.getColumnName();
        }).collect(Collectors.toList());
        // add extra pks
        List<String> prikeys = split.getLayout().getPrimaryKeys();
        for (String item : prikeys) {
            if (!dataCols.contains(item)) {
                LakeSoulTableColumnHandle columnHandle = (LakeSoulTableColumnHandle) allcolumns.get(item);
                fields.add(Field.nullable(columnHandle.getColumnName(),
                        ArrowUtil.convertToArrowType(columnHandle.getColumnType())));
            }
        }
        // add extra cdc column
        String
                cdcColumn =
                this.recordSet.getSplit().getLayout().getTableParameters().getString(PrestoUtil.CDC_CHANGE_COLUMN);
        if (cdcColumn != null) {
            fields.add(Field.notNullable(cdcColumn, new ArrowType.Utf8()));
        }

        reader.setPrimaryKeys(prikeys);
        reader.setSchema(new Schema(fields));
        for (Map.Entry<String, String> partition : this.partitions.entrySet()) {
            reader.setDefaultColumnValue(partition.getKey(), partition.getValue());
        }
        desiredTypes =
                recordSet.getColumnHandles()
                        .stream()
                        .map(item -> ((LakeSoulTableColumnHandle) item).getColumnType())
                        .collect(Collectors.toList());
        // set filters
        this.recordSet.getSplit().getLayout().getFilters().forEach((filter) -> reader.addFilter(filter.toString()));
        // set s3 options
        reader.setObjectStoreOptions(
                LakeSoulConfig.getInstance().getAccessKey(),
                LakeSoulConfig.getInstance().getAccessSecret(),
                LakeSoulConfig.getInstance().getRegion(),
                LakeSoulConfig.getInstance().getBucketName(),
                LakeSoulConfig.getInstance().getEndpoint(),
                LakeSoulConfig.getInstance().getDefaultFS(),
                LakeSoulConfig.getInstance().getUser()
        );

        // init reader
        reader.initializeReader();
        this.reader = new LakeSoulArrowReader(reader,
                100000);
        if (this.reader.hasNext()) {
            this.currentVCR = this.reader.nextResultVectorSchemaRoot();
            curRecordIdx = -1;
        } else {
            close();
            return;
        }
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        return desiredTypes.get(field);
    }

    @Override
    public boolean advanceNextPosition() {
        String
                cdcColumn =
                this.recordSet.getSplit().getLayout().getTableParameters().getString(PrestoUtil.CDC_CHANGE_COLUMN);
        if (cdcColumn != null) {
            while (next()) {
                FieldVector vector = currentVCR.getVector(cdcColumn);
                if (!vector.getObject(curRecordIdx).toString().equals("delete")) {
                    return true;
                }
            }
            return false;
        } else {
            return next();
        }
    }

    private boolean next() {
        if (currentVCR == null) {
            return false;
        }
        if (curRecordIdx >= currentVCR.getRowCount() - 1) {
            if (this.reader.hasNext()) {
                this.currentVCR = this.reader.nextResultVectorSchemaRoot();
                makeCurrentArrowReader();
                curRecordIdx = 0;
                return true;
            } else {
                this.reader.close();
                return false;
            }
        }
        curRecordIdx++;

        return true;
    }

    public void makeCurrentArrowReader() {

    }

    @Override
    public boolean getBoolean(int field) {
        return ((BitVector) this.currentVCR.getVector(field)).get(curRecordIdx) != 0;
    }

    @Override
    public long getLong(int field) {
        FieldVector fv = this.currentVCR.getVector(field);
        if (fv instanceof BigIntVector) {
            return ((BigIntVector) fv).get(curRecordIdx);
        }
        if (fv instanceof IntVector) {
            return ((IntVector) fv).get(curRecordIdx);
        }
        if (fv instanceof TinyIntVector) {
            return ((TinyIntVector) fv).get(curRecordIdx);
        }
        if (fv instanceof SmallIntVector) {
            return ((SmallIntVector) fv).get(curRecordIdx);
        }
        if (fv instanceof DateDayVector) {
            return ((DateDayVector) fv).get(curRecordIdx);
        }
        if (fv instanceof TimeStampMicroTZVector) {
            String timeZone = LakeSoulConfig.getInstance().getTimeZone();
            if (timeZone.equals("") || !Arrays.asList(TimeZone.getAvailableIDs()).contains(timeZone)) {
                timeZone = TimeZone.getDefault().getID();
            }
            return DateTimeEncoding.packDateTimeWithZone(((TimeStampMicroTZVector) fv).get(curRecordIdx) / 1000,
                    ZoneId.of(timeZone).toString());
        }
        if (fv instanceof DecimalVector) {
            BigDecimal dv = ((DecimalVector) fv).getObject(curRecordIdx);
            return Decimals.encodeShortScaledValue(dv, ((DecimalVector) fv).getScale());
        }
        if (fv instanceof Float4Vector) {
            return Float.floatToIntBits(((Float4Vector) fv).get(curRecordIdx));
        }
        throw new IllegalArgumentException("Field " + field + " is not a number, but is a " + fv.getClass().getName());
    }

    @Override
    public double getDouble(int field) {
        FieldVector fv = this.currentVCR.getVector(field);
        if (fv instanceof Float8Vector) {
            return ((Float8Vector) fv).get(curRecordIdx);
        }
        if (fv instanceof Float4Vector) {
            return ((Float4Vector) fv).get(curRecordIdx);
        }
        throw new IllegalArgumentException("Field " + field + " is not a float, but is a " + fv.getClass().getName());
    }

    @Override
    public Slice getSlice(int field) {
        Object value = getObject(field);
        requireNonNull(value, "value is null");
        if (value instanceof byte[]) {
            return Slices.wrappedBuffer((byte[]) value);
        }
        if (value instanceof String) {
            return Slices.utf8Slice((String) value);
        }
        if (value instanceof Slice) {
            return (Slice) value;
        }
        if (value instanceof Text) {
            return Slices.wrappedBuffer(((Text) value).getBytes());
        }
        if (value instanceof BigDecimal) {
            return Decimals.encodeScaledValue((BigDecimal) value);
        }
        throw new IllegalArgumentException("Field " +
                field +
                " is not a String, but is a " +
                value.getClass().getName());
    }

    @Override
    public Object getObject(int field) {
        return this.currentVCR.getVector(field).getObject(curRecordIdx);
    }

    @Override
    public boolean isNull(int field) {
        return currentVCR.getVector(field).isNull(curRecordIdx);
    }

    @Override
    public void close() {
        if (this.currentVCR != null) {
            this.currentVCR.close();
            this.currentVCR = null;
        }
    }
}
