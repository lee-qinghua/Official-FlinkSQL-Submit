--kafka授信申请表
    CREATE TABLE source1 (
      SESSION_ID STRING,
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
--kafka授信结果表
   CREATE TABLE source2 (
      SESSION_ID STRING,
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

-- source1不开窗口聚合
create view view1 as
select BUSINESS_TYPE_CD,count(1) as cnt1,max(APPLY_TIME) as ts from source1 group by BUSINESS_TYPE_CD;

-- source2不开窗口聚合
create view view2 as
select BUSINESS_TYPE_CD,count(1) as cnt2 from source2 group by BUSINESS_TYPE_CD;

-- source1/2 两个不开窗聚合结果 join
create view view3 as
select view1.BUSINESS_TYPE_CD,ts,cnt1,cnt2 from view1 join view2 on view1.BUSINESS_TYPE_CD=view2.BUSINESS_TYPE_CD;


-- 先把结果插入kafka再读，为什么这么做？因为再view中没办法指定时间属性，必须有时间属性才能和开窗聚合的几个属性进行interval join
   CREATE TABLE no_window_sink (
        id string,
        ts bigint,
        cnt1 bigint,
        cnt2 bigint
      )
       WITH (
      'connector.type' = 'kafka',
	  'connector.version' = 'universal',
      'connector.topic' = 'nowindow',
      'connector.properties.group.id'='dev_flink',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append'
    );
insert into no_window_sink select * from view3;

-- read
    CREATE TABLE no_window_source (
        id string,
        ts bigint,
        cnt1 bigint,
        cnt2 bigint,
        et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),
        WATERMARK FOR et AS et - INTERVAL '5' SECOND
    )
    WITH (
      'connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.topic' = 'nowindow',
      'connector.properties.group.id'='dev_flink',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append',
      'connector.startup-mode' = 'latest-offset'
      );

-- source1窗口聚合
create view view4 as
select
    BUSINESS_TYPE_CD,
    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR) AS dt,
    count (1) as apply_cnt_1h
from source1 GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD;

-- source2窗口聚合
create view view5 as
select
    BUSINESS_TYPE_CD,
    HOP_ROWTIME(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR) AS dt,
    count (1) as result_cnt_1h
from source2 GROUP BY HOP(et,INTERVAL '10' SECOND,INTERVAL '1' HOUR),BUSINESS_TYPE_CD;

-- source1/2 窗口聚合结果 join
create view view6 as
select
    view4.BUSINESS_TYPE_CD,
    view4.dt,
    apply_cnt_1h,
    result_cnt_1h
from view4 join view5 on view4.BUSINESS_TYPE_CD=view5.BUSINESS_TYPE_CD and view4.dt between view5.dt - interval '2' second and view5.dt + interval '2' second;

-- 两种聚合 interval join 注意时间条件，输出的结果为，非窗口聚合的数据和窗口聚合结果上一个窗口的结合
create view view7 as
select
    b.id,
    b.et,
    b.cnt1,
    b.cnt2,
    a.apply_cnt_1h,
    a.result_cnt_1h
from view6 a join no_window_source b on a.BUSINESS_TYPE_CD=b.id and a.dt between b.et - interval '10' second and b.et ;


   CREATE TABLE final_sink (
        id string,
        a TIMESTAMP(3),
        cnt1 bigint,
        cnt2 bigint,
        apply_cnt_1h    bigint,
        result_cnt_1h   bigint
      )
       WITH (
      'connector.type' = 'kafka',
	  'connector.version' = 'universal',
      'connector.topic' = 'test',
      'connector.properties.group.id'='dev_flink',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append'
    );

insert into final_sink select * from view7;