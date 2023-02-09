/*
 *
 *
 *   Copyright [2022] [DMetaSoul Team]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.spark.entry;

import java.util.ArrayList;
import java.util.List;

public class PipeLineContainer {
    private Resource resource;
    private List<Opreator> steps;
    private PipelineSink sink;

    public List<Opreator> getSteps() {
        return steps;
    }

    public void setSteps(List<Opreator> steps) {
        this.steps = steps;
    }

    public PipelineSink getSink() {
        return sink;
    }

    public void setSink(PipelineSink sink) {
        this.sink = sink;
    }

    public PipeLineContainer() {

    }

    public Resource getResource() {
        return resource;
    }

    public void setResource(Resource resource) {
        this.resource = resource;
    }
}


class Opreator {
    private String viewName;
    private String sourceTableName;
    private String sourceDatabaseName = "";
    private String sinkTableName;
    private SourceOption sourceOption;
    private Operation operation;

    public SourceOption getSourceOption() {
        return sourceOption;
    }

    public String getSourceDatabaseName() {
        return sourceDatabaseName;
    }

    public void setSourceDatabaseName(String sourceDatabaseName) {
        this.sourceDatabaseName = sourceDatabaseName;
    }

    public void setSourceOption(SourceOption sourceOption) {
        this.sourceOption = sourceOption;
    }

    public String getViewName() {
        return viewName;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    public String getSinkTableName() {
        return sinkTableName;
    }

    public void setSinkTableName(String sinkTableName) {
        this.sinkTableName = sinkTableName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public Operation getOperation() {
        return operation;
    }

    public String getTableNameWithDatabase() {
        String tableName = "";
        if ("".equals(sourceDatabaseName)) {
            tableName = sourceTableName;
        } else {
            tableName = sourceDatabaseName + "." + sourceTableName;
        }
        return tableName;
    }

    public String toSql() {
        String sql = "";
        String tableName = sourceTableName;
        if (operation.isGroupBy()) {
            String agg = operation.getGroupby().aggKeysSql();
            String metric = operation.getGroupby().metricKeysSql();
            sql = "select " + agg + "," + metric + " from " + tableName + " group by " + agg;
        }
        if (operation.isFilter()) {
            sql = "select * from " + tableName + " where " + operation.getFilter().toString();
        }
        if (operation.isJoin()) {
            sql = "select * from " + tableName + " " + operation.getJoin().getJoinType() + " join " + operation.getJoin().getRightTableName() + operation.getJoin().joinConditions();
        }
        return sql;
    }
}

class SourceOption {
    private String readStartTime = "1970-01-01 00:00:00";
    private String processType = "stream";

    public String getReadStartTime() {
        return readStartTime;
    }

    public void setReadStartTime(String readStartTime) {
        this.readStartTime = readStartTime;
    }

    public String getProcessType() {
        return processType;
    }

    public void setProcessType(String processType) {
        this.processType = processType;
    }
}

class Operation {
    private GroupBy groupby;
    private Join join;
    private Filter filter;

    public GroupBy getGroupby() {
        return groupby;
    }

    public void setGroupby(GroupBy groupby) {
        this.groupby = groupby;
    }

    public Join getJoin() {
        return join;
    }

    public void setJoin(Join join) {
        this.join = join;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public boolean isGroupBy() {
        return null != groupby;
    }

    public boolean isJoin() {
        return null != join;
    }

    public boolean isFilter() {
        return null != filter;
    }
}

class GroupBy {
    private List<String> aggKeys;
    private List<MetricsKeys> metricsKeys;

    public List<String> getAggKeys() {
        return aggKeys;
    }

    public void setAggKeys(List<String> aggKeys) {
        this.aggKeys = aggKeys;
    }

    public List<MetricsKeys> getMetricsKeys() {
        return metricsKeys;
    }

    public void setMetricsKeys(List<MetricsKeys> metricsKeys) {
        this.metricsKeys = metricsKeys;
    }

    public String aggKeysSql() {
        return String.join(",", aggKeys);
    }

    public String metricKeysSql() {
        List metrics = new ArrayList<String>();
        metricsKeys.forEach(metricsKey -> metrics.add(metricsKey.toString()));
        return String.join(",", metrics);
    }
}

class MetricsKeys {
    private String name;
    private String function;
    private String alias;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public String toString() {
        return function + "(" + name + ") as " + alias;
    }
}

class Join {
    private String rightTableName;
    private String joinType;
    private String conditionColumns;

    public String getRightTableName() {
        return rightTableName;
    }

    public void setRightTableName(String rightTableName) {
        this.rightTableName = rightTableName;
    }

    public String getJoinType() {
        return joinType;
    }

    public void setJoinType(String joinType) {
        this.joinType = joinType;
    }

    public String getConditionColumns() {
        return conditionColumns;
    }

    public void setConditionColumns(String conditionColumns) {
        this.conditionColumns = conditionColumns;
    }

    public String joinConditions() {
        String result = "";
        if (conditionColumns.contains("=")) {
            String[] condititions = conditionColumns.split(",");
            List<String> columns = new ArrayList<>();

            for (String item : condititions) {
                String[] cols = item.split("=");
                String left = cols[0].substring(cols[0].indexOf("."));
                String right = cols[1].substring(cols[1].indexOf("."));
                if (left.equals(right)) {
                    columns.add(left);
                } else {
                    result = " on " + conditionColumns;
                    break;
                }
            }
            result = " using(" + String.join(",", columns) + ")";
        } else {
            result = " using(" + conditionColumns + ")";
        }
        return result;
    }
}

class Filter {
    private String condititions;

    public String getCondititions() {
        return condititions;
    }

    public void setCondititions(String condititions) {
        this.condititions = condititions;
    }

    @Override
    public String toString() {
        return condititions;
    }
}

class PipelineSink {
    private String sinkTableName;
    private String sinkPath;
    private int triggerTime = 2000;
    private List<String> hashPartition;
    private int hashBucketNum = 2;
    private String rangPatition = "";
    private String outputmode = "complete";
    private String processType = "stream";
    private String checkpointLocation = "/tmp/chk";

    public PipelineSink() {

    }

    public String getRangPatition() {
        return rangPatition;
    }

    public void setRangPatition(String rangPatition) {
        this.rangPatition = rangPatition;
    }

    public List<String> getHashPartition() {
        return hashPartition;
    }

    public void setHashPartition(List<String> hashPartition) {
        this.hashPartition = hashPartition;
    }


    public String getSinkTableName() {
        return sinkTableName;
    }

    public void setSinkTableName(String sinkTableName) {
        this.sinkTableName = sinkTableName;
    }

    public String getSinkPath() {
        return sinkPath;
    }

    public void setSinkPath(String sinkPath) {
        this.sinkPath = sinkPath;
    }

    public int getTriggerTime() {
        return triggerTime;
    }

    public void setTriggerTime(int triggerTime) {
        this.triggerTime = triggerTime;
    }

    public int getHashBucketNum() {
        return hashBucketNum;
    }

    public void setHashBucketNum(int hashBucketNum) {
        this.hashBucketNum = hashBucketNum;
    }

    public String getOutputmode() {
        return outputmode;
    }

    public void setOutputmode(String outputmode) {
        this.outputmode = outputmode;
    }

    public String getProcessType() {
        return processType;
    }

    public void setProcessType(String processType) {
        this.processType = processType;
    }

    public String getCheckpointLocation() {
        return checkpointLocation;
    }

    public void setCheckpointLocation(String checkpointLocation) {
        this.checkpointLocation = checkpointLocation;
    }
}

class Resource {
    private int excutorNum = 1;
    private int excutorCores = 1;
    private String excutorMemory = "1g";
    private int driverCores = 1;
    private String driverMemory = "1g";

    public Resource() {

    }

    public int getExcutorNum() {
        return excutorNum;
    }

    public void setExcutorNum(int excutorNum) {
        this.excutorNum = excutorNum;
    }

    public int getExcutorCores() {
        return excutorCores;
    }

    public void setExcutorCores(int excutorCores) {
        this.excutorCores = excutorCores;
    }

    public String getExcutorMemory() {
        return excutorMemory;
    }

    public void setExcutorMemory(String excutorMemory) {
        this.excutorMemory = excutorMemory;
    }

    public int getDriverCores() {
        return driverCores;
    }

    public void setDriverCores(int driverCores) {
        this.driverCores = driverCores;
    }

    public String getDriverMemory() {
        return driverMemory;
    }

    public void setDriverMemory(String driverMemory) {
        this.driverMemory = driverMemory;
    }
}