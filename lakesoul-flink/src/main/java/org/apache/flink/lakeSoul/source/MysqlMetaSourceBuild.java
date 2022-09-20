package org.apache.flink.lakeSoul.source;

import java.util.HashSet;
import java.util.List;

public class MysqlMetaSourceBuild {
    private  String DBName;
    private String user;
    private String passwd;
    private HashSet<String> excludeTables=new HashSet<>();
    String host="127.0.0.1";
    String port="3306";
    public MysqlMetaSourceBuild user(String user){
        this.user=user;
        return this;
    }
    public MysqlMetaSourceBuild DatabaseName(String DBName){
        this.DBName=DBName;
        return this;
    }
    public MysqlMetaSourceBuild passwd(String passwd){
        this.passwd=passwd;
        return this;
    }
    public MysqlMetaSourceBuild port(String port){
        this.port=port;
        return this;
    }
    public MysqlMetaSourceBuild host(String host){
        this.host=host;
        return this;
    }
    public MysqlMetaSourceBuild excludeTables(List<String> tables){
        this.excludeTables.addAll(tables);
        return this;
    }
    public MysqlMetaDataSource build(){
        return new MysqlMetaDataSource(this.DBName, this.user, this.passwd,this.host,this.port,this.excludeTables);
    }

}
