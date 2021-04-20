drop table if exists mayi_info_1;
CREATE TABLE mayi_info_1 (
    CERT_NO string,
    APPLY_TIME bigint,
    APP_NO string,
    SPOUSE_CERT_NO string,
    MOBILE_NUM string,
    COMP_NAME string,
    STANDARD_ADDR string,
    proc as PROCTIME()
    )
    WITH (
      'connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.topic' = 'mayi_info',
      'connector.properties.group.id'='dev_flink1',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append',
      'connector.startup-mode' = 'latest-offset'
);

-- 结果表
drop table if exists result_table;
CREATE TABLE result_table (
CERT_NO string,
seven_days_cert_apply_num bigint,
 seven_days_cert_or_mobile_num bigint,
 seven_days_company_apply_num bigint,
  seven_days_address_apply_num bigint
    )
    WITH (
      'connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.topic' = 'result',
      'connector.properties.zookeeper.connect' = '10.1.30.6:2181',
      'connector.properties.bootstrap.servers' = '10.1.30.8:9092',
      'format.type' = 'json',
      'update-mode' = 'append'
);
-- 测试身份证关联次数
drop view if exists a;
create view a as
select
    proc,
    CERT_NO,
    APPLY_TIME,
    count (APP_NO) OVER w as cnt
from mayi_info_1
    window w as (partition by CERT_NO ORDER BY proc RANGE BETWEEN interval '7' DAY preceding and current row );

-- 测试身份证或手机号关联次数
drop view if exists d;
create view d as
select
    APPLY_TIME,
    MOBILE_NUM,
    count (APP_NO) OVER w as cnt
from mayi_info_1
    window w as (partition by MOBILE_NUM ORDER BY proc RANGE BETWEEN interval '7' DAY preceding and current row );

drop view if exists ad;
create view ad as
select
    APPLY_TIME,
    CERT_NO,
    MOBILE_NUM,
    count (APP_NO) OVER w as cnt
from mayi_info_1
    window w as (partition by MOBILE_NUM,CERT_NO ORDER BY proc RANGE BETWEEN interval '7' DAY preceding and current row );

drop view if exists e;
-- 测试同一单位申请次数
create view e as
select
    proc,
    APPLY_TIME,
    COMP_NAME,
    count (APP_NO) OVER w as cnt
from mayi_info_1
    window w as (partition by COMP_NAME ORDER BY proc RANGE BETWEEN interval '7' DAY preceding and current row );

drop view if exists f;
-- 测试7天内同一地址申请次数
create view f as
select
    APPLY_TIME,
    STANDARD_ADDR,
    count (APP_NO) OVER w as cnt
from mayi_info_1
    window w as (partition by STANDARD_ADDR ORDER BY proc RANGE BETWEEN interval '7' DAY preceding and current row );



