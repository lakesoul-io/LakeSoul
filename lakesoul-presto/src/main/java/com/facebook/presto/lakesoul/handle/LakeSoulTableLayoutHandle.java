// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.handle;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.*;
import com.facebook.presto.lakesoul.type.FloatType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slice;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.sql.types.LongType;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class LakeSoulTableLayoutHandle implements ConnectorTableLayoutHandle {
    private final LakeSoulTableHandle tableHandle;
    private final Optional<Set<ColumnHandle>> dataColumns;
    private final  List<String> primaryKeys;
    private final List<String> rangeKeys;
    private final JSONObject tableParameters;

    private final HashMap<String, ColumnHandle> allColumns;
    private final List<FilterPredicate> filters;
    private final TupleDomain<ColumnHandle> tupleDomain;
    @JsonCreator
    public LakeSoulTableLayoutHandle(
            @JsonProperty("tableHandle") LakeSoulTableHandle tableHandle,
            @JsonProperty("dataColumns") Optional<Set<ColumnHandle>> dataColumns,
            @JsonProperty("primaryKeys") List<String> primaryKeys,
            @JsonProperty("rangeKeys") List<String> rangeKeys,
            @JsonProperty("tableParameters") JSONObject tableParameters,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("allColumns") HashMap<String, ColumnHandle> allColumns


    ){
        this.tableHandle = requireNonNull(tableHandle, "tableHandle should not be null");
        this.dataColumns = requireNonNull(dataColumns, "dataColumns should not be null");
        this.primaryKeys = requireNonNull(primaryKeys, "primaryKeys should not be null");
        this.rangeKeys = requireNonNull(rangeKeys, "rangeKeys should not be null");
        this.tableParameters = requireNonNull(tableParameters, "tableParameters should not be null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain should not be null");
        this.allColumns = requireNonNull(allColumns, "allColumns should not be null");
        this.filters = buildFilters();
    }


    @JsonProperty
    public Optional<Set<ColumnHandle>> getDataColumns() {
        return dataColumns;
    }

    @JsonProperty
    public LakeSoulTableHandle getTableHandle() {
        return tableHandle;
    }
    @JsonProperty
    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }
    @JsonProperty
    public List<String> getRangeKeys() {
        return rangeKeys;
    }

    @JsonProperty
    public JSONObject getTableParameters() {
        return tableParameters;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }
    @JsonProperty
    public HashMap<String, ColumnHandle> getAllColumns() {
        return allColumns;
    }

    public List<FilterPredicate> getFilters() {
        return filters;
    }

    private List<FilterPredicate> buildFilters(){
        List<FilterPredicate> query = new LinkedList<>();
        if (tupleDomain.getDomains().isPresent()) {
            for (Map.Entry<ColumnHandle, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
                LakeSoulTableColumnHandle column = (LakeSoulTableColumnHandle) entry.getKey();
                query.add(buildPredicate(column, entry.getValue()));
            }
        }
        return query;
    }

    private FilterPredicate buildPredicate(LakeSoulTableColumnHandle column, Domain domain){
        // 暂时实现了 整数和浮点数
        String name = column.getColumnName();
        Type type = column.getColumnType();
        if (domain.getValues().isNone() && domain.isNullAllowed()) {
            return eq(type, name, null);
        }
        if (domain.getValues().isAll() && !domain.isNullAllowed()) {
            return FilterApi.not(eq(type, name, null));
        }

        List<Object> singleValues = new ArrayList<>();
        List<FilterPredicate> disjuncts = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            if (range.isSingleValue()) {
                singleValues.add(range.getSingleValue());
            }
            else {
                FilterPredicate rangeConjuncts = null;
                if (!range.isLowUnbounded()) {
                    if(range.isLowInclusive()){
                        rangeConjuncts = gte(type, name, range.getLowBoundedValue());
                    }else{
                        rangeConjuncts = gt(type, name, range.getLowBoundedValue());
                    }
                }
                if (!range.isHighUnbounded()) {
                    if(range.isHighInclusive()){
                        rangeConjuncts = lte(type, name, range.getHighBoundedValue());
                    }else{
                        rangeConjuncts = lt(type, name, range.getHighBoundedValue());
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                if(rangeConjuncts != null){
                    disjuncts.add(rangeConjuncts);
                }
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() > 0) {
            for(Object value : singleValues){
                disjuncts.add(eq(type, name, value));
            }
        }

        if (domain.isNullAllowed()) {
            disjuncts.add(eq(type, name, null));
        }

        return disjuncts.stream().reduce(FilterApi::or).get();
    }

    private FilterPredicate eq(Type type, String name, Object value){
        if (type instanceof IntegerType
                || type instanceof SmallintType
                || type instanceof TinyintType
        ) {
            if(value == null) {
                return FilterApi.eq ( FilterApi.intColumn (name), null);
            }
            if(!(value instanceof Long)){
                throw new RuntimeException("except filter value type is long, but it is " + value.getClass());
            }
            return FilterApi.eq ( FilterApi.intColumn (name), ((Long) value).intValue());
        } else if(type instanceof LongType){
            if(value == null) {
                return FilterApi.eq ( FilterApi.longColumn (name), null);
            }
            if(!(value instanceof Long)){
                throw new RuntimeException("except filter value type is long, but it is " + value.getClass());
            }
            return FilterApi.eq ( FilterApi.longColumn (name), ((Long) value));
        }  else if(type instanceof FloatType){
            if(value == null) {
                return FilterApi.eq ( FilterApi.floatColumn (name), null);
            }
            if(!(value instanceof Double)){
                throw new RuntimeException("except filter value type is double, but it is " + value.getClass());
            }
            return FilterApi.eq ( FilterApi.floatColumn (name), ((Double) value).floatValue());
        }else if(type instanceof DoubleType){
            if(value == null) {
                return FilterApi.eq ( FilterApi.doubleColumn (name), null);
            }
            if(!(value instanceof Double)){
                throw new RuntimeException("except filter value type is double, but it is " + value.getClass());
            }
            return FilterApi.eq ( FilterApi.doubleColumn (name), ((Double) value));
        }  else if(type instanceof BooleanType){
            if(value == null) {
                return FilterApi.eq ( FilterApi.booleanColumn(name), null);
            }
            if(!(value instanceof Boolean)){
                throw new RuntimeException("except filter value type is boolean, but it is " + value.getClass());
            }
            return FilterApi.eq ( FilterApi.booleanColumn (name), ((Boolean) value));
        }else if(type instanceof DecimalType){
            if(value == null) {
                return FilterApi.eq ( FilterApi.binaryColumn (name), null);
            }
            // decimal predicate pushdown is not fully supported
        } else if(type instanceof VarcharType){
            if(value == null) {
                return FilterApi.eq ( FilterApi.binaryColumn (name), null);
            }
            if(!(value instanceof Slice)){
                throw new RuntimeException("except filter value type is string, but it is " + value.getClass());
            }

            return FilterApi.eq (
                    FilterApi.binaryColumn (name),
                    Binary.fromString(((Slice) value).toStringUtf8()));
        }
        throw new RuntimeException("unsupport data type:" + type);
    }


    private FilterPredicate gt(Type type, String name, Object value){
        if (type instanceof IntegerType
                || type instanceof SmallintType
                || type instanceof TinyintType
        ) {
            if(!(value instanceof Long)){
                throw new RuntimeException("except filter value type is long, but it is " + value.getClass());
            }
            return FilterApi.gt ( FilterApi.intColumn (name), ((Long) value).intValue());
        } else if(type instanceof BigintType){
            if(!(value instanceof Long)){
                throw new RuntimeException("except filter value type is long, but it is " + value.getClass());
            }
            return FilterApi.gt ( FilterApi.longColumn (name), ((Long) value));
        }  else if(type instanceof FloatType){
            if(!(value instanceof Double)){
                throw new RuntimeException("except filter value type is double, but it is " + value.getClass());
            }
            return FilterApi.gt ( FilterApi.floatColumn (name), ((Double) value).floatValue());
        }else if(type instanceof DoubleType){
            if(!(value instanceof Double)){
                throw new RuntimeException("except filter value type is double, but it is " + value.getClass());
            }
            return FilterApi.gt ( FilterApi.doubleColumn (name), ((Double) value));
        }
        throw new RuntimeException("unsupport data type:" + type);
    }

    private FilterPredicate gte(Type type, String name, Object value){
        if (type instanceof IntegerType
                || type instanceof SmallintType
                || type instanceof TinyintType
        ) {
            if(!(value instanceof Long)){
                throw new RuntimeException("except filter value type is long, but it is " + value.getClass());
            }
            return FilterApi.gtEq ( FilterApi.intColumn (name), ((Long) value).intValue());
        } else if(type instanceof LongType){
            if(!(value instanceof Long)){
                throw new RuntimeException("except filter value type is long, but it is " + value.getClass());
            }
            return FilterApi.gtEq ( FilterApi.longColumn (name), ((Long) value));
        }  else if(type instanceof FloatType){
            if(!(value instanceof Double)){
                throw new RuntimeException("except filter value type is double, but it is " + value.getClass());
            }
            return FilterApi.gtEq ( FilterApi.floatColumn (name), ((Double) value).floatValue());
        }else if(type instanceof DoubleType){
            if(!(value instanceof Double)){
                throw new RuntimeException("except filter value type is double, but it is " + value.getClass());
            }
            return FilterApi.gtEq ( FilterApi.doubleColumn (name), ((Double) value));
        }
        throw new RuntimeException("unsupport data type:" + type);
    }

    private FilterPredicate lt(Type type, String name, Object value){
        if (type instanceof IntegerType
                || type instanceof SmallintType
                || type instanceof TinyintType
        ) {
            if(!(value instanceof Long)){
                throw new RuntimeException("except filter value type is long, but it is " + value.getClass());
            }
            return FilterApi.lt ( FilterApi.intColumn (name), ((Long) value).intValue());
        } else if(type instanceof LongType){
            if(!(value instanceof Long)){
                throw new RuntimeException("except filter value type is long, but it is " + value.getClass());
            }
            return FilterApi.lt ( FilterApi.longColumn (name), ((Long) value));
        }  else if(type instanceof FloatType){
            if(!(value instanceof Double)){
                throw new RuntimeException("except filter value type is double, but it is " + value.getClass());
            }
            return FilterApi.lt ( FilterApi.floatColumn (name), ((Double) value).floatValue());
        }else if(type instanceof DoubleType){
            if(!(value instanceof Double)){
                throw new RuntimeException("except filter value type is double, but it is " + value.getClass());
            }
            return FilterApi.lt ( FilterApi.doubleColumn (name), ((Double) value));
        }
        throw new RuntimeException("unsupport data type:" + type);
    }

    private FilterPredicate lte(Type type, String name, Object value){
        if (type instanceof IntegerType
                || type instanceof SmallintType
                || type instanceof TinyintType
        ) {
            if(!(value instanceof Long)){
                throw new RuntimeException("except filter value type is long, but it is " + value.getClass());
            }
            return FilterApi.ltEq ( FilterApi.intColumn (name), ((Long) value).intValue());
        } else if(type instanceof LongType){
            if(!(value instanceof Long)){
                throw new RuntimeException("except filter value type is long, but it is " + value.getClass());
            }
            return FilterApi.ltEq ( FilterApi.longColumn (name), ((Long) value));
        }  else if(type instanceof FloatType){
            if(!(value instanceof Double)){
                throw new RuntimeException("except filter value type is double, but it is " + value.getClass());
            }
            return FilterApi.ltEq ( FilterApi.floatColumn (name), ((Double) value).floatValue());
        }else if(type instanceof DoubleType){
            if(!(value instanceof Double)){
                throw new RuntimeException("except filter value type is double, but it is " + value.getClass());
            }
            return FilterApi.ltEq ( FilterApi.doubleColumn (name), ((Double) value));
        }
        throw new RuntimeException("unsupport data type:" + type);
    }


}