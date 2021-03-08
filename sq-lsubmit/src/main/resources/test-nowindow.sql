
CREATE TABLE kafka_apply_info (
                                      SESSION_ID STRING,
                                      BUSINESS_TYPE_CD STRING,
                                      APPLY_TIME BIGINT,
                                      et AS TO_TIMESTAMP(FROM_UNIXTIME(APPLY_TIME/1000)),
                                      WATERMARK FOR et AS et - INTERVAL '5' SECOND
)
WITH (
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'kafkaCreditApplyInfo',
    'connector.properties.group.id'='dev_flink',
    'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
    'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
    'format.type' = 'json',
    'update-mode' = 'append',
    'connector.startup-mode' = 'latest-offset'
);

--kafka授信结果表
CREATE TABLE kafka_result_info (
                                       SESSION_ID STRING,
                                       BUSINESS_TYPE_CD STRING,
                                       CREDIT_CODE  STRING,
                                       WHITE_LIST int, -- 命中白名单标识 1 命中 0 未命中
                                       CREDIT_TIME  BIGINT,
                                       et AS TO_TIMESTAMP(FROM_UNIXTIME(CREDIT_TIME/1000)),
                                       WATERMARK FOR et AS et - INTERVAL '5' SECOND
)WITH (
      'connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.topic' = 'kafkaCreditResultInfo_1234',
      'connector.properties.group.id'='dev_flink',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append',
      'connector.startup-mode' = 'latest-offset'
    );

-- 申请表
create view view1 as
select
    BUSINESS_TYPE_CD,
    count(SESSION_ID) as APP_C_CT -- 截止当前时点前贷款授信申请笔数
from kafka_apply_info group by BUSINESS_TYPE_CD;

-- 结果表
create view view2 as
select
    BUSINESS_TYPE_CD,
    sum(if(CREDIT_CODE=0,1,0)) 	as 		ACC_C_CT,
    sum(if(WHITE_LIST=1,1,0)) 	as 		APP_C_WL_CCT
from kafka_result_info group by BUSINESS_TYPE_CD;

-- 申请/结果 表join
create view view3 as
select
    view1.BUSINESS_TYPE_CD,
    APP_C_CT,
    ACC_C_CT,
    APP_C_WL_CCT
from view1 join view2 on view1.BUSINESS_TYPE_CD=view2.BUSINESS_TYPE_CD;

-- 结果表 开窗
create view view4 as
select
    BUSINESS_TYPE_CD,
    hop_rowtime(et,interval '20' second,interval '1' minute) as dt,
    sum(if(CREDIT_CODE=0,1,0)) 	as 		ACC_C_CT_1m,
    sum(if(WHITE_LIST=1,1,0)) 	as 		APP_C_WL_CCT_1m
from kafka_result_info group by BUSINESS_TYPE_CD,hop(et,interval '20' second,interval '1' minute);


create view5 as
select
view3.BUSINESS_TYPE_CD,
APP_C_CT,
ACC_C_CT,
APP_C_WL_CCT,
ACC_C_CT_1m,
APP_C_WL_CCT_1m
from view3 join view4 on view3.BUSINESS_TYPE_CD=view4.BUSINESS_TYPE_CD;



create table sink_table(
BUSINESS_TYPE_CD string,
APP_C_CT BIGINT,
ACC_C_CT BIGINT,
APP_C_WL_CCT BIGINT
)with(
    'connector.type' = 'kafka',
    'connector.version' = 'universal',
    'connector.topic' = 'kafkaProductMonitor1',
    'connector.properties.group.id'='dev_flink',
    'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
    'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
    'format.type' = 'json',
    'update-mode' = 'append'
);

insert into sink_table
select
    BUSINESS_TYPE_CD,
    APP_C_CT,
    ACC_C_CT,
    APP_C_WL_CCT
from view3;


