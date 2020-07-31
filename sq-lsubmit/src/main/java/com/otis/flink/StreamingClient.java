package com.otis.flink;

import com.otis.flink.sqlSubmit.SqlSubmit;
import com.otis.flink.sqlSubmit.UserOptions;
import com.otis.flink.sqlSubmit.UserOptionsParser;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class StreamingClient {
    public static void main(String[] args) throws Exception {
        final UserOptions options = UserOptionsParser.parseUserArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置checkpoint
        // env.enableCheckpointing(1000 * 60 * 10);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
        SqlSubmit submit = new SqlSubmit(options, tableEnvironment);
        submit.run();
    }
}
