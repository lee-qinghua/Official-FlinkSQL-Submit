package com.otis.flink.udaf;


import org.apache.flink.table.functions.ScalarFunction;

public class TestFunction extends ScalarFunction {
    public String eval(String name) {
        return name.toUpperCase();
    }
}
