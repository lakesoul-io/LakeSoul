/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package org.apache.flink.lakesoul.source;

import java.util.HashSet;
import java.util.List;

public class MysqlMetaSourceBuild {
    private String DBName;
    private String user;
    private String passwd;
    private final HashSet<String> excludeTables = new HashSet<>();
    String host = "127.0.0.1";
    String port = "3306";

    public MysqlMetaSourceBuild user(String user) {
        this.user = user;
        return this;
    }

    public MysqlMetaSourceBuild DatabaseName(String DBName) {
        this.DBName = DBName;
        return this;
    }

    public MysqlMetaSourceBuild passwd(String passwd) {
        this.passwd = passwd;
        return this;
    }

    public MysqlMetaSourceBuild port(String port) {
        this.port = port;
        return this;
    }

    public MysqlMetaSourceBuild host(String host) {
        this.host = host;
        return this;
    }

    public MysqlMetaSourceBuild excludeTables(List<String> tables) {
        this.excludeTables.addAll(tables);
        return this;
    }

    public MysqlMetaDataSource build() {
        return new MysqlMetaDataSource(this.DBName, this.user, this.passwd, this.host, this.port, this.excludeTables);
    }

}
