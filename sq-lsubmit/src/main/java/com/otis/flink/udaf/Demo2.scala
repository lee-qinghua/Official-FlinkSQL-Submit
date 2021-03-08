package com.otis.flink.udaf

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

    // 开启后不是没来一条数据更新一次 比如count(1)，而是10s聚合一次或者5000条数据更新一次结果
    // 设置mini batch 用于不开窗group by的优化，减少状态的访问压力
    val configuration = tableEnv.getConfig.getConfiguration
    // set low-level key-value options
    configuration.setString("table.exec.mini-batch.enabled", "true")
    configuration.setString("table.exec.mini-batch.allow-latency", "10 s")
    configuration.setString("table.exec.mini-batch.size", "5000")


    tableEnv.registerFunction("customcount", new CustomCount)
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


    tableEnv.createTemporaryView("view1", tableEnv.sqlQuery("select BUSINESS_TYPE_CD,count(1) as cnt1,max(APPLY_TIME) as ts from kafka_apply_info_200 group by BUSINESS_TYPE_CD"))
    tableEnv.createTemporaryView("view2", tableEnv.sqlQuery("select BUSINESS_TYPE_CD,count(1) as cnt2 from kafka_result_info_200 group by BUSINESS_TYPE_CD"))
    tableEnv.createTemporaryView("view3", tableEnv.sqlQuery("select view1.BUSINESS_TYPE_CD,ts,cnt1,cnt2 from view1 join view2 on view1.BUSINESS_TYPE_CD=view2.BUSINESS_TYPE_CD"))

    val mid_sink =
      """
        | CREATE TABLE no_window_sink (
        |        id string,
        |        ts bigint,
        |        cnt1 bigint,
        |        cnt2 bigint
        |      )
        |       WITH (
        |      'connector.type' = 'kafka',
        |	  'connector.version' = 'universal',
        |      'connector.topic' = 'nowindow',
        |      'connector.properties.group.id'='dev_flink',
        |      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
        |      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
        |      'format.type' = 'json',
        |      'update-mode' = 'append'
        |    )
        |""".stripMargin
    tableEnv.sqlUpdate(mid_sink)
    tableEnv.sqlUpdate("insert into no_window_sink select * from view3")
    val no_window_source =
      """
        |    CREATE TABLE no_window_source (
        |        id string,
        |        ts bigint,
        |        cnt1 bigint,
        |        cnt2 bigint,
        |        et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),
        |        WATERMARK FOR et AS et - INTERVAL '5' SECOND
        |    )
        |    WITH (
        |      'connector.type' = 'kafka',
        |      'connector.version' = 'universal',
        |      'connector.topic' = 'nowindow',
        |      'connector.properties.group.id'='dev_flink',
        |      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
        |      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
        |      'format.type' = 'json',
        |      'update-mode' = 'append',
        |      'connector.startup-mode' = 'latest-offset'
        |      )
        |      """.stripMargin
    tableEnv.sqlUpdate(no_window_source)


    tableEnv.createTemporaryView("view4", tableEnv.sqlQuery("select\n    BUSINESS_TYPE_CD,\n    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR) AS dt,\n    count (1) as apply_cnt_1h\nfrom kafka_apply_info_200 GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD"))
    tableEnv.createTemporaryView("view5", tableEnv.sqlQuery("select\n    BUSINESS_TYPE_CD,\n    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR) AS dt,\n    count (1) as result_cnt_1h\nfrom kafka_result_info_200 GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD"))
    tableEnv.createTemporaryView("view6", tableEnv.sqlQuery("select\nview4.BUSINESS_TYPE_CD,\nview4.dt,\napply_cnt_1h,\nresult_cnt_1h\nfrom view4 join view5 on view4.BUSINESS_TYPE_CD=view5.BUSINESS_TYPE_CD and view4.dt between view5.dt - interval '2' second and view5.dt + interval '2' second"))
    tableEnv.createTemporaryView("view7", tableEnv.sqlQuery("select\n    b.id,\n    b.et,\n    b.cnt1,\n    b.cnt2,\n    a.apply_cnt_1h,\n    a.result_cnt_1h\nfrom view6 a join no_window_source b on a.BUSINESS_TYPE_CD=b.id and a.dt between b.et - interval '10' second and b.et"))
    val final_sink =
      """
        |   CREATE TABLE final_sink (
        |        id string,
        |        a TIMESTAMP(3),
        |        cnt1 bigint,
        |        cnt2 bigint,
        |        apply_cnt_1h    bigint,
        |        result_cnt_1h   bigint
        |      )
        |       WITH (
        |      'connector.type' = 'kafka',
        |	  'connector.version' = 'universal',
        |      'connector.topic' = 'test',
        |      'connector.properties.group.id'='dev_flink',
        |      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
        |      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
        |      'format.type' = 'json',
        |      'update-mode' = 'append'
        |    )""".stripMargin
    tableEnv.sqlUpdate(final_sink)
//    tableEnv.sqlUpdate("insert into final_sink select * from view7")
    tableEnv.sqlQuery("select * from view7").toAppendStream[Row].print()

    env.execute("")
  }
}
