


-- 测试 ok 15点51分




--kafka授信申请表
    CREATE TABLE kafka_apply_info_200 (
      SESSION_ID STRING,
      APP_NO STRING,
      CUST_ID STRING,
      BUSINESS_TYPE_CD STRING,
      BUSINESS_TYPE_NAME STRING,
      CUST_SOURCE STRING,
      CHANNEL_SOURCE STRING,
      APPLY_TIME BIGINT,
      et AS TO_TIMESTAMP(FROM_UNIXTIME(APPLY_TIME/1000)),
      WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )
    WITH (
      'connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.topic' = 'kafkaCreditApplyInfo_1234',
      'connector.properties.group.id'='dev_flink',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append',
      'connector.startup-mode' = 'latest-offset'
      );

create view view1 as select BUSINESS_TYPE_CD,count(1) as cnt1,max(APPLY_TIME) as ts from kafka_apply_info_200 group by BUSINESS_TYPE_CD;
create view view2 as select BUSINESS_TYPE_CD,count (1) as cnt2 from kafka_apply_info_200 group by BUSINESS_TYPE_CD;

create view view3 as select view1.BUSINESS_TYPE_CD,cnt1,cnt2, ts from view1 join view2 on view1.BUSINESS_TYPE_CD=view2.BUSINESS_TYPE_CD;

--kafka sink表
   CREATE TABLE kafka_product_monitor_suixindai (
        id string,
        cnt1 bigint,
        cnt2 bigint,
        ts BIGINT
      )
       WITH (
      'connector.type' = 'kafka',
	  'connector.version' = 'universal',
      'connector.topic' = 'kafkaProductMonitor',
      'connector.properties.group.id'='dev_flink',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append'
    );

insert into kafka_product_monitor_suixindai select * from view3;