// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.handle;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;

import java.util.*;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class LakeSoulTableLayoutHandle implements ConnectorTableLayoutHandle {
    private final LakeSoulTableHandle tableHandle;
    private final Optional<Set<ColumnHandle>> dataColumns;
    private final  List<String> primaryKeys;
    private final List<String> rangeKeys;
    private final JSONObject tableParameters;

    private final List<FilterPredicate> filters;

    private final TupleDomain<ColumnHandle> tupleDomain;
    @JsonCreator
    public LakeSoulTableLayoutHandle(
            @JsonProperty("tableHandle") LakeSoulTableHandle tableHandle,
            @JsonProperty("dataColumns") Optional<Set<ColumnHandle>> dataColumns,
            @JsonProperty("primaryKeys") List<String> primaryKeys,
            @JsonProperty("rangeKeys") List<String> rangeKeys,
            @JsonProperty("tableParameters") JSONObject tableParameters,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain
            ){
        this.tableHandle = requireNonNull(tableHandle, "tableHandle should not be null");
        this.dataColumns = requireNonNull(dataColumns, "dataColumns should not be null");
        this.primaryKeys = requireNonNull(primaryKeys, "primaryKeys should not be null");
        this.rangeKeys = requireNonNull(rangeKeys, "rangeKeys should not be null");
        this.tableParameters = requireNonNull(tableParameters, "tableParameters should not be null");
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain should not be null");
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
        // 先以int为例
        // TODO: 后边再慢慢补吧
        String name = column.getColumnName();
        Type type = column.getColumnType();
        if (domain.getValues().isNone() && domain.isNullAllowed()) {
            return FilterApi.eq(FilterApi.intColumn(name), null);
        }
        if (domain.getValues().isAll() && !domain.isNullAllowed()) {
            return FilterApi.not(FilterApi.eq(FilterApi.intColumn(name), null));
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
                        rangeConjuncts = FilterApi.gtEq(FilterApi.intColumn(name), ((Long) range.getLowBoundedValue()).intValue());
                    }else{
                        rangeConjuncts = FilterApi.gt(FilterApi.intColumn(name), ((Long) range.getLowBoundedValue()).intValue());
                    }
                }
                if (!range.isHighUnbounded()) {
                    if(range.isHighInclusive()){
                        rangeConjuncts = FilterApi.ltEq(FilterApi.intColumn(name), ((Long) range.getHighBoundedValue()).intValue());
                    }else{
                        rangeConjuncts = FilterApi.lt(FilterApi.intColumn(name), ((Long) range.getHighBoundedValue()).intValue());
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
                disjuncts.add(FilterApi.eq(FilterApi.intColumn(name), ((Long) value).intValue()));
            }
        }

        if (domain.isNullAllowed()) {
            disjuncts.add(FilterApi.eq(FilterApi.intColumn(name), null));
        }

        return disjuncts.stream().reduce(FilterApi::or).get();
    }
}
