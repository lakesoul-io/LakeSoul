// THIS FILE IS OVERWRITE BY THE zhp8341/FLINK-STREAMING-PLATFORM-WEB PROJECT, UNDER MIT LICENSE.

// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry.sql.flink;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.command.SetOperation;

import java.util.ArrayList;
import java.util.List;

public class ExecuteSql {
    public static void exeSql(List<String> sqlList, TableEnvironment tEnv) {
        Parser parser = ((TableEnvironmentInternal) tEnv).getParser();
        List<ModifyOperation> modifyOperationList = new ArrayList<>();

        for (String stmtOri : sqlList) {
            String stmt = trimBlank(stmtOri);
            Operation operation = parser.parse(stmt).get(0);

            // flink version 1.14.5
            switch (operation.getClass().getSimpleName()) {
                case "PlannerQueryOperation":
                case "ShowTablesOperation":
                case "ShowCatalogsOperation":
                case "ShowCreateTableOperation":
                case "ShowCurrentCatalogOperation":
                case "ShowCurrentDatabaseOperation":
                case "ShowDatabasesOperation":
                case "ShowFunctionsOperation":
                case "ShowModulesOperation":
                case "ShowPartitionsOperation":
                case "ShowViewsOperation":
                case "ExplainOperation":
                case "DescribeTableOperation":
                    tEnv.executeSql(stmt).print();
                    break;

                //set
                case "SetOperation":
                    SetOperation setOperation = (SetOperation) operation;
                    Configurations.setSingleConfiguration(tEnv, setOperation.getKey().get(),
                            setOperation.getValue().get());
                    break;

                case "BeginStatementSetOperation":
                case "EndStatementSetOperation":
                    break;

                case "DropTableOperation":
                case "DropCatalogFunctionOperation":
                case "DropTempSystemFunctionOperation":
                case "DropCatalogOperation":
                case "DropDatabaseOperation":
                case "DropViewOperation":
                case "CreateTableOperation":
                case "CreateViewOperation":
                case "CreateDatabaseOperation":
                case "CreateCatalogOperation":
                case "CreateTableASOperation":
                case "CreateCatalogFunctionOperation":
                case "CreateTempSystemFunctionOperation":
                case "AlterTableOperation":
                case "AlterViewOperation":
                case "AlterDatabaseOperation":
                case "AlterCatalogFunctionOperation":
                case "UseCatalogOperation":
                case "UseDatabaseOperation":
                case "LoadModuleOperation":
                case "UnloadModuleOperation":
                case "NopOperation": {
                    ((TableEnvironmentInternal) tEnv).executeInternal(operation).print();
                    break;
                }
                // insert
                case "ModifyOperation":
                    modifyOperationList.add((ModifyOperation) operation);
                    break;
                default:
                    throw new RuntimeException("not support sql=" + stmt);
            }
        }
        int modifyOperationListLength = modifyOperationList.size();
        if (modifyOperationListLength == 0) {
            return;
        }
        ((TableEnvironmentInternal) tEnv).executeInternal(modifyOperationList).print();
    }

    private static String trimBlank(String str) {
        return str.replace("\\n", " ").replaceAll("\\s+", " ").trim();
    }
}
