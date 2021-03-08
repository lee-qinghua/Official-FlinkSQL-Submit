package com.otis.flink.udaf;

import org.apache.flink.table.functions.AggregateFunction;

public class CustomAvg extends AggregateFunction<Double, CustomAvg.AccObj> {

    @Override
    public Double getValue(AccObj accumulator) {
        return accumulator.sum / accumulator.count;
    }

    @Override
    public AccObj createAccumulator() {
        AccObj obj = new AccObj();
        obj.count = 0;
        obj.sum = 0.0;
        return obj;
    }


    /**
     * 一小时时间窗内的贷款授信申请通过客户的客户卡评分均值
     *
     * @param acc       累加器
     * @param score     评分
     * @param isSuccess 是否通过审核
     */
    public void accumulate(AccObj acc, double score, boolean isSuccess) {
        if (isSuccess) {
            acc.sum = acc.sum + score;
            acc.count = acc.count + 1;
        }
    }

    public static class AccObj {
        public double sum;
        public int count;
    }
}
