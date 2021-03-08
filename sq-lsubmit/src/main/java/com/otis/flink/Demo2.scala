package com.otis.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object Demo2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val source1 =
      """
        |    CREATE TABLE kafka_apply_info_200 (
        |      SESSION_ID STRING,
        |      APP_NO STRING,
        |      CUST_ID STRING,
        |      BUSINESS_TYPE_CD STRING,
        |      BUSINESS_TYPE_NAME STRING,
        |      CUST_SOURCE STRING,
        |      CHANNEL_SOURCE STRING,
        |      APPLY_TIME BIGINT,
        |      et AS TO_TIMESTAMP(FROM_UNIXTIME(APPLY_TIME/1000)),
        |      WATERMARK FOR et AS et - INTERVAL '5' SECOND
        |    )
        |    WITH (
        |      'connector.type' = 'kafka',
        |      'connector.version' = 'universal',
        |      'connector.topic' = 'kafkaCreditApplyInfo_1234',
        |      'connector.properties.group.id'='dev_flink',
        |      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
        |      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
        |      'format.type' = 'json',
        |      'update-mode' = 'append',
        |      'connector.startup-mode' = 'latest-offset'
        |      )
        |""".stripMargin
    tableEnv.sqlUpdate(source1)

    val source2 =
      """
        |   CREATE TABLE kafka_result_info_200 (
        |      SESSION_ID STRING,
        |      APP_NO  STRING,
        |      CUST_ID STRING,
        |      CREDIT_NO  STRING,
        |      BUSINESS_TYPE_CD STRING,
        |      STATE_CODE STRING,
        |      CREDIT_CODE  STRING,
        |      REFUSE_REASON STRING,
        |      INTEREST_RATE  DOUBLE,
        |      CREDIT_LIMIT DOUBLE,
        |     REPAY_MODE_CD STRING,
        |     LOAN_TERM INTEGER,
        |     CREDIT_SCORE_1  DOUBLE, -- 客户卡评分
        |     CREDIT_SCORE_2 DOUBLE,
        |     CREDIT_SCORE_3  DOUBLE,
        |     ANTI_FRAUD_SCORE_1  DOUBLE,
        |     ANTI_FRAUD_SCORE_2  DOUBLE,
        |     WHITE_LIST int, -- 命中白名单标识 1 命中 0 未命中
        |     APPLY_SCORE int,--产品卡评分
        |     BLACK_LIST int, -- 命中黑名单标识 1 命中 0 未命中
        |     CREDIT_TIME  BIGINT,
        |     et AS TO_TIMESTAMP(FROM_UNIXTIME(CREDIT_TIME/1000)),
        |    WATERMARK FOR et AS et - INTERVAL '5' SECOND
        |    )WITH (
        |      'connector.type' = 'kafka',
        |      'connector.version' = 'universal',
        |      'connector.topic' = 'kafkaCreditResultInfo_1234',
        |      'connector.properties.group.id'='dev_flink',
        |      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
        |      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
        |      'format.type' = 'json',
        |      'update-mode' = 'append',
        |      'connector.startup-mode' = 'latest-offset'
        |    )
        |""".stripMargin
    tableEnv.sqlUpdate(source2)

    val now_view1 =
      """
        |select
        |    BUSINESS_TYPE_CD,
        |    count (1) over w as APP_C_CT,
        |    MAX(SUBSTR(DATE_FORMAT(et, 'yyyy-MM-dd HH:mm:ss'),1,18) || '0') OVER w AS time_str,
        |    et
        |from  kafka_apply_info_200 where BUSINESS_TYPE_CD='suixindai'
        |window w as (order by et ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        |""".stripMargin
    tableEnv.createTemporaryView("now_view1", tableEnv.sqlQuery(now_view1))

    val now_view1_10 =
      """
        |select
        |time_str,
        |max(et) as et,
        |BUSINESS_TYPE_CD,
        |max(APP_C_CT) as APP_C_CT
        |from now_view1 group by BUSINESS_TYPE_CD,time_str
        |""".stripMargin
    tableEnv.createTemporaryView("now_view1_10", tableEnv.sqlQuery(now_view1_10))

    val now_view2 =
      """
        |select
        |    et,
        |    MAX(SUBSTR(DATE_FORMAT(et, 'yyyy-MM-dd HH:mm:ss'),1,18) || '0') OVER w AS time_str,
        |    BUSINESS_TYPE_CD,
        |    sum (if(CREDIT_CODE='0',1,0))                       over w as      ACC_C_CT,       -- 截止当前时点前贷款授信申请通过笔数
        |    sum (if(WHITE_LIST=1,1,0))                          over w as      APP_C_WL_CCT,   -- 截止当前时点前贷款授信申请且命中白名单客户的笔数
        |    sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0))      over w as      ACC_C_WL_CCT,   -- 截止当前时点前贷款授信申请通过且命中白名单客户的笔数
        |    sum (if(WHITE_LIST=0,1,0))                          over w as      APP_C_UWL_CCT,  -- 截止当前时点前贷款授信申请且wei命中白名单客户的笔数
        |    sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0))      over w as      ACC_C_UWL_CCT,  -- 截止当前时点前贷款授信申请通过且wei命中白名单客户的笔数
        |    sum (if(BLACK_LIST=1,1,0))                          over w as      AF_ACC_C_CT,    -- 截止当前时点前贷款授信申请反欺诈通过笔数
        |    avg (CREDIT_SCORE_1)                                over w as      AVG_ACC_CCSCT,  -- 截止当前时点前贷款授信申请通过客户的客户卡评分均值
        |    avg(APPLY_SCORE)                                    over w as      AVG_ACC_PCSCT   -- 截止当前时点前贷款授信申请通过客户的产品卡评分均值
        |from kafka_result_info_200 where BUSINESS_TYPE_CD='suixindai' window w as (order by et ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)""".stripMargin
    tableEnv.createTemporaryView("now_view2", tableEnv.sqlQuery(now_view2))
    val now_view2_10 =
      """
        |select
        |time_str,
        |    max(et) as et,
        |    BUSINESS_TYPE_CD,
        |    max(ACC_C_CT) as ACC_C_CT,
        |    max(APP_C_WL_CCT) as APP_C_WL_CCT,
        |    max(ACC_C_WL_CCT) as ACC_C_WL_CCT,
        |    max(APP_C_UWL_CCT) as APP_C_UWL_CCT,
        |    max(ACC_C_UWL_CCT) as ACC_C_UWL_CCT,
        |    max(AF_ACC_C_CT) as AF_ACC_C_CT,
        |    max(AVG_ACC_CCSCT) as AVG_ACC_CCSCT,
        |    max(AVG_ACC_PCSCT) as AVG_ACC_PCSCT
        |from now_view2 group by BUSINESS_TYPE_CD,time_str
        |""".stripMargin
    tableEnv.createTemporaryView("now_view2_10", tableEnv.sqlQuery(now_view2_10))

    val now_view3=
      """
        |select
        |a.et,
        |a.BUSINESS_TYPE_CD,
        |a.ACC_C_CT,
        |b.APP_C_CT
        |from now_view2_10 a join now_view1_10 b on a.BUSINESS_TYPE_CD=b.BUSINESS_TYPE_CD and a.time_str=b.time_str and a.et between b.et-interval '2' second and b.et+interval '2' second
        |""".stripMargin

//    tableEnv.createTemporaryView("now_view3", tableEnv.sqlQuery(now_view3))

    tableEnv.sqlQuery("select * from now_view1").toAppendStream[Row].print()
    env.execute("")
  }
}
