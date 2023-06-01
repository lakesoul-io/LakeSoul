/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.meta;

import java.io.Serializable;

public class DataBaseProperty implements Serializable {
    private String driver;
    private String url;
    private String username;
    private String password;
    private int maxCommitAttempt;

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMaxCommitAttempt() {
        return maxCommitAttempt;
    }

    public void setMaxCommitAttempt(int maxCommitAttempt) {
        this.maxCommitAttempt = maxCommitAttempt;
    }

    @Override
    public String toString() {
        return "lakesoul.pg.driver=" +
                driver +
                "\nlakesoul.pg.url=" +
                url +
                "\nlakesoul.pg.username" +
                username +
                "\nlakesoul.pg.password" +
                password +
                "\n";
    }
}
