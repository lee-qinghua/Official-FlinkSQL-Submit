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
        |    CREATE TABLE kafka_apply_info (
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
        |      'connector.properties.group.id'='dev_flink2',
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
        |   CREATE TABLE kafka_result_info (
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
        |      'connector.properties.group.id'='dev_flink2',
        |      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
        |      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
        |      'format.type' = 'json',
        |      'update-mode' = 'append',
        |      'connector.startup-mode' = 'latest-offset'
        |    )
        |""".stripMargin
    tableEnv.sqlUpdate(source2)

    val sink =
      """
        | CREATE TABLE kafka_product_monitor_suixindai (
        |    BUSINESS_TYPE_CD        STRING,
        |    BUSINESS_TYPE_NAME      STRING,
        |    DT                      TIMESTAMP(3),
        |    APP_C_CT_1H	            BIGINT,
        |    APP_C_CT_2H	            BIGINT,
        |    APP_C_CT_6H	            BIGINT,
        |    ACC_C_CT_1H	            BIGINT,
        |    ACC_C_CT_2H	            BIGINT,
        |    ACC_C_CT_6H	            BIGINT,
        |    PR_CT_1H	            FLOAT,
        |    PR_CT_2H	            FLOAT,
        |    PR_CT_6H	            FLOAT,
        |    ACC_C_CCT_1H	        BIGINT,
        |    ACC_C_CCT_2H	        BIGINT,
        |    ACC_C_CCT_6H	        BIGINT,
        |    AVG_AC_CCT_1H	        DOUBLE,
        |    AVG_AC_CCT_2H	        DOUBLE,
        |    AVG_AC_CCT_6H	        DOUBLE,
        |    APP_C_WL_CCT_1H	        BIGINT,
        |    APP_C_WL_CCT_2H	        BIGINT,
        |    APP_C_WL_CCT_6H	        BIGINT,
        |    ACC_C_WL_CCT_1H	        BIGINT,
        |    ACC_C_WL_CCT_2H	        BIGINT,
        |    ACC_C_WL_CCT_6H	        BIGINT,
        |    PR_WL_CCT_1H	        FLOAT,
        |    PR_WL_CCT_2H	        FLOAT,
        |    PR_WL_CCT_6H	        FLOAT,
        |    APP_C_UWL_CCT_1H	    BIGINT,
        |    APP_C_UWL_CCT_2H	    BIGINT,
        |    APP_C_UWL_CCT_6H	    BIGINT,
        |    ACC_C_UWL_CCT_1H	    BIGINT,
        |    ACC_C_UWL_CCT_2H	    BIGINT,
        |    ACC_C_UWL_CCT_6H	    BIGINT,
        |    PR_UWL_CCT_1H	        FLOAT,
        |    PR_UWL_CCT_2H	        FLOAT,
        |    PR_UWL_CCT_6H	        FLOAT,
        |    AF_ACC_C_CT_1H	        BIGINT,
        |    AF_ACC_C_CT_2H	        BIGINT,
        |    AF_ACC_C_CT_6H	        BIGINT,
        |    AF_PR_CT_1H	            FLOAT,
        |    AF_PR_CT_2H	            FLOAT,
        |    AF_PR_CT_6H	            FLOAT,
        |    AVG_ACC_CCSCT_1H	    DOUBLE,
        |    AVG_ACC_CCSCT_2H	    DOUBLE,
        |    AVG_ACC_CCSCT_6H	    DOUBLE,
        |    AVG_ACC_PCSCT_1H	    DOUBLE,
        |    AVG_ACC_PCSCT_2H	    DOUBLE,
        |    AVG_ACC_PCSCT_6H	    DOUBLE,
        |    RATE_OC_1H	            DOUBLE,
        |    RATE_OC_2H	            DOUBLE,
        |    RATE_OC_6H	            DOUBLE
        |      )
        |       WITH (
        |      'connector.type' = 'kafka',
        |	  'connector.version' = 'universal',
        |      'connector.topic' = 'suixindai_monitor',
        |      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
        |      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
        |      'format.type' = 'json',
        |      'update-mode' = 'append'
        |    )
        |""".stripMargin
    tableEnv.sqlUpdate(sink)
    val s1 =
      """
        |create view kafka_apply_info_200 as
        |select * from kafka_apply_info where BUSINESS_TYPE_CD='P0036'
        |""".stripMargin

    val s2 =
      """
        |create view kafka_result_info_200 as
        |select * from kafka_result_info where BUSINESS_TYPE_CD='P0036'
        |""".stripMargin
    tableEnv.sqlUpdate(s1)
    tableEnv.sqlUpdate(s2)

    val apply_view1 =
      """
        |create view apply_view1 as
        |select
        |    BUSINESS_TYPE_CD,
        |    BUSINESS_TYPE_NAME,
        |    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR) AS dt,
        |    count (1) as APP_C_CT_1H
        |from kafka_apply_info_200 GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD,BUSINESS_TYPE_NAME
        |""".stripMargin
    tableEnv.sqlUpdate(apply_view1)

    tableEnv.sqlUpdate(
      """
        |create view apply_view2 as
        |select
        |BUSINESS_TYPE_CD,
        |    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '2' HOUR) AS dt,
        |    count (1) as APP_C_CT_2H
        |from kafka_apply_info_200 GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '2' HOUR),BUSINESS_TYPE_CD""".stripMargin)

    tableEnv.sqlUpdate(
      """
        |create view apply_view6 as
        |select
        |BUSINESS_TYPE_CD,
        |    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '6' HOUR) AS dt,
        |    count (1) as APP_C_CT_6H
        |from kafka_apply_info_200 GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '6' HOUR),BUSINESS_TYPE_CD""".stripMargin)

    tableEnv.sqlUpdate(
      """
        |create view mid_1 as
        |select
        |BUSINESS_TYPE_CD,
        |    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR)                                                                                                          as dt,
        |    sum (if(CREDIT_CODE='0',1,0))                                                                                                                                   as ACC_C_CT_1H,         -- 1小时时间窗内的贷款授信申请通过笔数
        |    -- cast (sum (if(CREDIT_CODE='0',1,0))/count (SESSION_ID) as float)                                                                                             as PR_CT_1H,            -- 1小时时间窗内的贷款授信申请通过笔数/申请笔数
        |    customcount(CUST_ID,CREDIT_CODE)                                                                                                                                as ACC_C_CCT_1H,        -- 1小时时间窗内的贷款授信申请通过客户数                   自定义函数求 集合的个数
        |    -- cast (customcount(CUST_ID,CREDIT_CODE)/count (SESSION_ID) as double)                                                                                         as AVG_AC_CCT_1H,       -- 1小时时间窗内的贷款授信申请通过客户数/申请笔数
        |    sum (if(WHITE_LIST=1,1,0))                                                                                                                                      as APP_C_WL_CCT_1H,     -- 1小时时间窗内的贷款授信申请且命中白名单客户的笔数
        |    sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0))                                                                                                                  as ACC_C_WL_CCT_1H,     -- 1小时时间窗内的贷款授信申请通过且命中白名单客户的笔数
        |    cast (if (sum(if(WHITE_LIST=1,1,0))=0,-999,cast (sum (if(CREDIT_CODE='0' and WHITE_LIST=1,1,0)) as float )/sum (if(WHITE_LIST=1,1,0))) as float)                                 as PR_WL_CCT_1H,        -- 1小时时间窗内的贷款授信申请通过且命中白名单客户的笔数/申请且命中白名单客户的笔数
        |    sum (if(WHITE_LIST=0,1,0))                                                                                                                                      as APP_C_UWL_CCT_1H,    -- 1小时时间窗内的贷款授信申请且未命中白名单客户的笔数
        |    sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0))                                                                                                                  as ACC_C_UWL_CCT_1H,    -- 1小时时间窗内的贷款授信申请通过且未命中白名单客户的笔数
        |    cast (if(sum(if(WHITE_LIST=0,1,0))=0,-999,cast(sum (if(CREDIT_CODE='0' and WHITE_LIST=0,1,0)) as float )/sum(if(WHITE_LIST=0,1,0))) as float )                                  as PR_UWL_CCT_1H,       -- 1小时时间窗内的贷款授信申请通过且未命中白名单客户的笔数/申请且未命中白名单客户的笔数
        |    sum (if(BLACK_LIST=0,1,0))                                                                                                                                      as AF_ACC_C_CT_1H,      -- 1小时时间窗内的贷款授信申请反欺诈通过笔数
        |    -- cast (sum (if(BLACK_LIST='0',1,0))/count (SESSION_ID) as float)                                                                                              as AF_PR_CT_1H,         -- 1小时时间窗内的贷款授信申请反欺诈通过笔数/申请笔数
        |    cast ( if (sum (if(CREDIT_CODE='0',1,0))=0,-999,sum(if(CREDIT_CODE='0',CREDIT_SCORE_1,0))/sum (if(CREDIT_CODE='0',1,0))) as double )                            as AVG_ACC_CCSCT_1H,    -- 1小时时间窗内的贷款授信申请通过客户的客户卡评分均值           自定义函数
        |    cast ( if (sum (if(CREDIT_CODE='0',1,0))=0,-999,sum(if(CREDIT_CODE='0',APPLY_SCORE,0))/sum (if(CREDIT_CODE='0',1,0))) as double )                               as AVG_ACC_PCSCT_1H,    -- 1小时时间窗内的贷款授信申请通过客户的产品卡评分均值
        |    cast ( if(customcount(CUST_ID,CREDIT_CODE)=0,-999,customcount(CUST_ID,CREDIT_CODE,if(CREDIT_SCORE_1 is null,0,CAST(CREDIT_SCORE_1 AS INT)),if(CREDIT_LIMIT is null,0.0,CREDIT_LIMIT))/customcount(CUST_ID,CREDIT_CODE)) as double)     as RATE_OC_1H           -- 1小时时间窗内的贷款授信申请通过客户的客户卡评分低于220分且额度高于4万元的客户数量/申请通过客户数量 自定义函数
        |from kafka_result_info_200 group by HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD""".stripMargin)


    tableEnv.sqlUpdate(
      """
        |create view mid_1h as
        |select
        |mid_1.BUSINESS_TYPE_CD,
        |BUSINESS_TYPE_NAME,
        |    apply_view1.dt,
        |    APP_C_CT_1H,
        |    ACC_C_CT_1H,
        |    if(APP_C_CT_1H=0,-999,cast (cast(ACC_C_CT_1H as float)/APP_C_CT_1H as float)) as PR_CT_1H,
        |    ACC_C_CCT_1H,
        |    if(APP_C_CT_1H=0,-999,cast (cast(ACC_C_CCT_1H as double )/APP_C_CT_1H as double )) as AVG_AC_CCT_1H,
        |    APP_C_WL_CCT_1H,
        |    ACC_C_WL_CCT_1H,
        |    PR_WL_CCT_1H,
        |    APP_C_UWL_CCT_1H,
        |    ACC_C_UWL_CCT_1H,
        |    PR_UWL_CCT_1H,
        |    AF_ACC_C_CT_1H,
        |    if(APP_C_CT_1H=0,-999,cast(cast(AF_ACC_C_CT_1H as float )/APP_C_CT_1H as float )) as AF_PR_CT_1H,
        |    AVG_ACC_CCSCT_1H,
        |    AVG_ACC_PCSCT_1H,
        |    RATE_OC_1H
        |from mid_1,apply_view1 where mid_1.BUSINESS_TYPE_CD=apply_view1.BUSINESS_TYPE_CD and mid_1.dt between apply_view1.dt - interval '2' second and apply_view1.dt + interval '2' second""".stripMargin)





    tableEnv.sqlQuery("select * from mid_1h").toAppendStream[Row].print()
    env.execute("")
  }
}
