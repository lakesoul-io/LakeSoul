// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.facebook.presto.lakesoul.handle.LakeSoulTransactionHandle;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.*;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class LakeSoulConnector implements Connector {

    private final LakeSoulMetadata metadata;
    private final LakeSoulSplitManager splitManager;
    private final LakeSoulRecordSetProvider recordSetProvider;


    public LakeSoulConnector(
            LakeSoulMetadata metadata,
            LakeSoulSplitManager splitManager,
            LakeSoulRecordSetProvider recordSetProvider
    ){
        this.metadata = requireNonNull(metadata, "metadata should not be null");
        this.splitManager = requireNonNull(splitManager, "splitManager should not be null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider should not be null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        return new LakeSoulTransactionHandle();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return recordSetProvider;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return Connector.super.getPageSourceProvider();
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider() {
        return Connector.super.getPageSinkProvider();
    }

    @Override
    public ConnectorIndexProvider getIndexProvider() {
        return Connector.super.getIndexProvider();
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider() {
        return Connector.super.getNodePartitioningProvider();
    }

    @Override
    public ConnectorPlanOptimizerProvider getConnectorPlanOptimizerProvider() {
        return Connector.super.getConnectorPlanOptimizerProvider();
    }

    @Override
    public ConnectorMetadataUpdaterProvider getConnectorMetadataUpdaterProvider() {
        return Connector.super.getConnectorMetadataUpdaterProvider();
    }

    @Override
    public ConnectorTypeSerdeProvider getConnectorTypeSerdeProvider() {
        return Connector.super.getConnectorTypeSerdeProvider();
    }

    @Override
    public Set<SystemTable> getSystemTables() {
        return Connector.super.getSystemTables();
    }

    @Override
    public Set<Procedure> getProcedures() {
        return Connector.super.getProcedures();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties() {
        return Connector.super.getSessionProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties() {
        return Connector.super.getSchemaProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getAnalyzeProperties() {
        return Connector.super.getAnalyzeProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties() {
        return Connector.super.getTableProperties();
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties() {
        return Connector.super.getColumnProperties();
    }

    @Override
    public ConnectorAccessControl getAccessControl() {
        return Connector.super.getAccessControl();
    }

    @Override
    public ConnectorCommitHandle commit(ConnectorTransactionHandle transactionHandle) {
        return Connector.super.commit(transactionHandle);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle) {
        Connector.super.rollback(transactionHandle);
    }

    @Override
    public boolean isSingleStatementWritesOnly() {
        return Connector.super.isSingleStatementWritesOnly();
    }

    @Override
    public void shutdown() {
        Connector.super.shutdown();
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities() {
        return Connector.super.getCapabilities();
    }
}
