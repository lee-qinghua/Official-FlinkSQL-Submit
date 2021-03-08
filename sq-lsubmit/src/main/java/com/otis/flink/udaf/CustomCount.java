package com.otis.flink.udaf;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.HashSet;


public class CustomCount extends AggregateFunction<Integer, HashSet<String>> {

    public void merge(HashSet<String> accumulator, Iterable<HashSet<String>> iterable) {
        iterable.forEach(accumulator::addAll);
    }

    @Override
    public Integer getValue(HashSet<String> accumulator) {
        return accumulator.size();
    }

    @Override
    public HashSet<String> createAccumulator() {
        return new HashSet<String>();
    }

    /**
     * 一小时时间窗内的贷款授信申请通过客户数
     *
     * @param acc        累加器
     * @param custId     客户id
     * @param creditCode 审核通过结果
     */
    public void accumulate(HashSet<String> acc, String custId, String creditCode) {
        if ("0".equals(creditCode)) {
            acc.add(custId);
        }
    }

    /**
     * 一小时时间窗内的贷款授信申请通过客户的客户卡评分低于220分且额度高于4万元的客户数量
     *
     * @param acc          累加器
     * @param custId       客户id
     * @param creditCode   审核通过结果
     * @param creditScore1 客户卡评分
     * @param creditLimit  额度
     */
    public void accumulate(HashSet<String> acc, String custId, String creditCode, int creditScore1, double creditLimit) {
        if ("0".equals(creditCode) && creditScore1 < 220 && creditLimit > 40000) {
            acc.add(custId);
        }
    }

}
