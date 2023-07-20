// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.rbac;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.GlobalConfig;
import com.dmetasoul.lakesoul.meta.dao.DataCommitInfoDao;
import com.dmetasoul.lakesoul.meta.entity.CommitOp;
import com.dmetasoul.lakesoul.meta.entity.DataCommitInfo;
import com.dmetasoul.lakesoul.meta.entity.DataFileOp;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.junit.After;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

//public class AuthZDomainTestDataCommitTest extends AuthZDomainTest{
//
//    protected final String TEST_NAME_SPACE = "authz_test_name_space_test_data_commit";
//
//
//    protected final String TEST_TABLE_ID = "016A6D2D-1BA1-5412-DB98-6DFFFE6975DA";
//
//
//    protected final String TEST_TABLE_NAME = "016A6D2D-1BA1-5412-DB98-6DFFFE6975DA";
//
//    @Test
//    public void testSingle(){
//        GlobalConfig.get().setAuthZEnabled(true);
//        // create a table for test
//        dbManager.createNewTable(
//                TEST_TABLE_ID,
//                TEST_NAME_SPACE,
//                TEST_TABLE_NAME,
//                TEST_TABLE_PATH,
//                "schema",
//                new JSONObject(),
//                "");
//        // commit data
//        List<DataFileOp> ops = new LinkedList<>();
//
//        dbManager.commitDataCommitInfo(
//                DataCommitInfo.newBuilder()
//                        .setTableId(TEST_TABLE_ID)
//                        .setPartitionDesc("part")
//                        .setCommitId(TEST_TABLE_ID)
//                        .addAllFileOps(ops)
//                        .setCommitOp(CommitOp.AppendCommit)
//                        .setCommitted(true)
//                        .setTimestamp(0)
//                        .build()
//        );
//
//        DataCommitInfoDao dao = new DataCommitInfoDao();
//        DataCommitInfo dataCommitInfo = dao.selectByTableId(TEST_TABLE_ID);
//        assert dataCommitInfo.getDomain().equals("test1");
//    }
//
//    @Test
//    public void testBatch(){
//        GlobalConfig.get().setAuthZEnabled(true);
//        // create a table for test
//        dbManager.createNewTable(
//                TEST_TABLE_ID,
//                TEST_NAME_SPACE,
//                TEST_TABLE_NAME,
//                TEST_TABLE_PATH,
//                "schema",
//                new JSONObject(),
//                "");
//        // commit data
//        List<DataFileOp> ops = new LinkedList<>();
//        List<DataCommitInfo> infos = new LinkedList<>();
//        infos.add(DataCommitInfo.newBuilder()
//                .setTableId(TEST_TABLE_ID)
//                .setPartitionDesc("part")
//                .setCommitId(TEST_TABLE_ID)
//                .addAllFileOps(ops)
//                .setCommitOp(CommitOp.AppendCommit)
//                .setCommitted(true)
//                .setTimestamp(0)
//                .build());
//
//        dbManager.batchCommitDataCommitInfo(infos);
//
//        DataCommitInfoDao dao = new DataCommitInfoDao();
//        DataCommitInfo dataCommitInfo = dao.selectByTableId(TEST_TABLE_ID);
//        assert dataCommitInfo.getDomain().equals("test1");
//    }
//
//
//
//    @After
//    public void clean(){
//        dbManager.deleteTableInfo(TEST_TABLE_PATH, TEST_TABLE_ID, TEST_NAME_SPACE);
//        dbManager.deleteDataCommitInfo(TEST_TABLE_ID);
//    }
//}

