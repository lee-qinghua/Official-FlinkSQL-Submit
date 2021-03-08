package com.otis.flink.sqlSubmit;

import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlSubmit {


    private String sqlFilePath;
    private String workSpace;
//    private String hiveConfPath;
    private StreamTableEnvironment tEnv;

    public SqlSubmit(CliOptions options, StreamTableEnvironment tEnv) {
        this.sqlFilePath = options.getSqlFilePath();
        this.workSpace = options.getWorkingSpace();
//        this.hiveConfPath = options.getHiveConfPath();
        this.tEnv=tEnv;
    }

    public void run() throws Exception {

//        HiveCatalog hive = new HiveCatalog("myHive",
//                "flink",
//                hiveConfPath,
//                "1.1.0");
//        tEnv.registerCatalog("myhive", hive);
//        tEnv.useCatalog("myhive");

        List<String> sql = Files.readAllLines(Paths.get(workSpace + "/" + sqlFilePath));
        List<SqlCommandParser.SqlCommandCall> calls = SqlCommandParser.parse(sql);
        for (SqlCommandParser.SqlCommandCall call : calls) {
            callCommand(call);
        }
        tEnv.execute("Streaming SQL Job");
    }

    // --------------------------------------------------------------------------------------------

    private void callCommand(SqlCommandParser.SqlCommandCall cmdCall) {
        switch (cmdCall.command) {
            case SET:
                callSet(cmdCall);
                break;
            case CREATE_TABLE:
                callCreateTable(cmdCall);
                break;
            case INSERT_INTO:
                callInsertInto(cmdCall);
                break;
            /*case QUERY_TABLE:
                callQueryTable(cmdCall);
                break;*/
            case CREATE_VIEW:
                callCreateView(cmdCall);
                break;
            case DROP_FUNCTION:
                callDropFunction(cmdCall);
                break;
            case CREATE_FUNCTION:
                callCreateFunction(cmdCall);
                break;
            default:
                throw new RuntimeException("Unsupported command: " + cmdCall.command);
        }
    }

    private void callDropFunction(SqlCommandParser.SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            tEnv.sqlUpdate(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
        System.out.println("添加获取的删除function的sql是"+ ddl);
    }

    private void callCreateFunction(SqlCommandParser.SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            tEnv.sqlUpdate(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
        System.out.println("添加获取的创建function的sql是"+ ddl);
    }

    private void callSet(SqlCommandParser.SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        tEnv.getConfig().getConfiguration().setString(key, value);
    }

    private void callCreateTable(SqlCommandParser.SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            tEnv.sqlUpdate(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
        System.out.println("获取的sql创建语句是"+ddl);
    }

    private void callInsertInto(SqlCommandParser.SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];
        try {
            tEnv.sqlUpdate(dml);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }
        System.out.println("添加获取的sql是"+dml);
    }

    private void callCreateView(SqlCommandParser.SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];
        //Pattern pattern=new Pattern();
        Pattern pattern=Pattern.compile("(?<= as).*",Pattern.DOTALL|Pattern.CASE_INSENSITIVE);
        Pattern pattern1=Pattern.compile("(?<=view ).*?(?= as)",Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(dml);
        Matcher matcher1 = pattern1.matcher(dml);
        if (matcher.find()&matcher1.find()){
            String sqlquery = matcher.group(0);
            String viewName = matcher1.group(0);
            System.out.println("获取的sql语句是"+sqlquery+"视图名是"+viewName);
            tEnv.createTemporaryView(viewName,tEnv.sqlQuery(sqlquery));
        }else {
            throw new RuntimeException("Unsupported command '" + dml + "'");
        }
        //System.out.println("获取的sql语句是:"+dml);
    }


   /* private void callQueryTable(SqlCommandParser.SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];
        System.out.println("获取的sql"+dml);
        try {
            DataStream<Tuple2<Boolean, Result3>> stream = tEnv.toRetractStream(tEnv.sqlQuery(dml), Result3.class);
            tEnv.registerDataStream("newResult",stream);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }
    }*/
}
