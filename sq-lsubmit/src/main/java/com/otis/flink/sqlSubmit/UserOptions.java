package com.otis.flink.sqlSubmit;

public class UserOptions {

    private final String sqlFilePath;

    public UserOptions(String sqlFilePath) {
        this.sqlFilePath = sqlFilePath;
    }

    public String getSqlFilePath() {
        return sqlFilePath;
    }


}
