
    CREATE TABLE mayi_info_1 (
      SESSION_ID                 STRING,
      APP_NO                     STRING,
      BUSINESS_TYPE_CD           STRING,
      BUSINESS_TYPE_NAME         STRING,
      APPLY_TIME                 BIGINT,
      STEP_TYPE                  STRING,    --流程类型
      NODE_TYPE                  STRING,    --节点类型
      CUST_SOURCE                STRING,    --来源系统
      CHANNEL_SOURCE             STRING,    --来源渠道
      CUST_ID                    STRING,    --客户编号
      CUST_NAME                  STRING,    --客户名称
      GENDER_CD                  STRING,    --性别代码
      BIRTH_DT                   STRING,    --出生日期
      CERT_TYPE                  STRING,    --证件类型代码
      CERT_NO                    STRING,    --证件号码
      INDE_VALID_DT              STRING,    --证件有效期
      CUSTOM_EVALU_LEVEL         STRING,    --我行vip用户
      NATIONALITY_CD             STRING,    --国籍代码
      APPL_CHAN                  STRING,    --守信申请渠道
      CERT_NO_2                  STRING,    --证件号码2
      CERT_TYPE_2                STRING,    --证件类型2
      CERT_VALID_END_DT          STRING,    --证件有效期
      NAME                       STRING,    --申请人姓名
      MOBILE_NO                  STRING,    --申请手机号
      PRODUCT_CODE               STRING,    --产品标识
      BIZ_MODE                   STRING,    --业务模式
      LOAN_MODE                  STRING,    --出资模式
      APPLY_TYPE                 STRING,    --申请类型
      SPOUSE_NAME                STRING,    --配偶姓名
      SPOUSE_CERT_NO             STRING,    --配偶身份证
      SPOUSE_MOBILE_NO           STRING,    --配偶电话
      COMP_NAME                  STRING,    --工作单位
      CUR_MARRIAGE_STATUS_CD     STRING,    --婚姻状态
      ADDR_DESC                  STRING,    --地址
      ADDR_USAGE_EDC             STRING,    --地址用途
      MOBILE_NUM                 STRING,    --手机号码
      COMP_NAME                  STRING,    --工作单位
      DUTY_CD                    STRING,    -- 职务代码
      TITLE_CD                   STRING,    --职称代码
      OCCUPATION_CD              STRING,    --职业代码
      EDUCATION_LEVEL_CD         STRING,    --学历代码
      CERT_ADDR                  STRING,    --身份证地址
      STANDARD_ADDR              STRING,    --标准化地址
      et AS TO_TIMESTAMP(FROM_UNIXTIME(APPLY_TIME/1000)),
      WATERMARK FOR et AS et - INTERVAL '5' SECOND
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

-- 7身份证关联申请信息个数
create view a as
select
    CERT_NO,
    APPLY_TIME,
    count (APP_NO) OVER w as cnt
from mayi_info_1
    where CERT_TYPE='1'
    window w as (partition by CERT_NO ORDER BY et RANGE BETWEEN interval '7' DAY preceding and current row );

--7天内手机号申请次数
create view d as
select
    et,
    APPLY_TIME,
    MOBILE_NUM,
    count (APP_NO) OVER w as cnt
from mayi_info_1
    window w as (partition by MOBILE_NUM ORDER BY et RANGE BETWEEN interval '7' DAY preceding and current row );
--7天内手机号&身份证号申请次数
create view ad as
select
    et,
    APPLY_TIME,
    CERT_NO,
    MOBILE_NUM,
    count (APP_NO) OVER w as cnt
from mayi_info_1
    window w as (partition by MOBILE_NUM,CERT_NO ORDER BY et RANGE BETWEEN interval '7' DAY preceding and current row );


--7天内同一单位名称申请贷款人数
create view e as
select
    et,
    APPLY_TIME,
    COMP_NAME,
    count (APP_NO) OVER w as cnt
from mayi_info_1
    window w as (partition by COMP_NAME ORDER BY et RANGE BETWEEN interval '7' DAY preceding and current row );

-- 7天内同一地址申请次数
create view f as
select
    et,
    APPLY_TIME,
    STANDARD_ADDR,
    count (APP_NO) OVER w as cnt
from mayi_info_1
    window w as (partition by STANDARD_ADDR ORDER BY et RANGE BETWEEN interval '7' DAY preceding and current row );



-- 选择CERT_NO 本人身份证的view
create view fake_data as
select
    et,
    APPLY_TIME,
    APP_NO,
    CERT_NO,
    'fake' as flag
from mayi_info_1;
-- 选择SPOUSE_CERT_NO 联系人身份证的view
create view real_data as
select
    et,
    APPLY_TIME,
    APP_NO,
    SPOUSE_CERT_NO,
    'real' as flag
from mayi_info_1;

-- 两个view union all 相当于把两个view中的数据合在一起
create view union_view as
select *
from
    (select et,APPLY_TIME,APP_NO,CERT_NO as id,flag from fake_data)
union all
     (select et,APPLY_TIME,APP_NO,SPOUSE_CERT_NO as id,flag from real_data);

-- 根据union_view 计算次数的时候，注意flag  sum(if(flag='fake',0,1))
create view union_data as
select
    id,
    et,
    APPLY_TIME,
    sum(if(flag='fake',0,1)) over(partition by id order by et range between interval '7' DAY preceding and current row) as cnt
from union_view;

-- phone number
create view fake_data_phone as
select
    et,
    APPLY_TIME,
    APP_NO,
    MOBILE_NUM,
    'fake' as flag
from mayi_info_1;

create view real_data_phone as
select
    et,
    APPLY_TIME,
    APP_NO,
    SPOUSE_MOBILE_NO,
    'real' as flag
from mayi_info_1;

create view union_view_phone as
select *
from
    (select et,APPLY_TIME,APP_NO,MOBILE_NUM as phone,flag from fake_data_phone)
union all
     (select et,APPLY_TIME,APP_NO,SPOUSE_MOBILE_NO as phone,flag from real_data_phone);

create view union_data_phone as
select
    phone,
    et,
    APPLY_TIME,
    sum(if(flag='fake',0,1)) over(partition by phone order by et range between interval '7' DAY preceding and current row) as cnt
from union_view_phone;


-- 最终结果
create view view1 as
select
    mayi_info_1.CERT_NO,
    a.cnt                  as 7days_cert_apply_num,
    a.cnt-ad.cnt+d.cnt     as 7days_cert_or_mobile_num,
    e.cnt                  as 7days_company_apply_num,
    f.cnt                  as 7days_address_apply_num,
    union_data.cnt         as 7days_cert_contact_num,
    union_data_phone.cnt   as 7days_mobile_contact_num
from mayi_info_1
join a on mayi_info_1.CERT_NO=a.CERT_NO and mayi_info_1.APPLY_TIME=a.APPLY_TIME and mayi_info_1.et between a.et -interval '2' second and a.et
join e on mayi_info_1.COMP_NAME=e.COMP_NAME and mayi_info_1.APPLY_TIME=e.APPLY_TIME and  mayi_info_1.et between e.et -interval '2' second and e.et
join f on mayi_info_1.STANDARD_ADDR=f.STANDARD_ADDR and mayi_info_1.APPLY_TIME=f.APPLY_TIME and mayi_info_1.et between f.et -interval '2' second and f.et
join d on mayi_info_1.MOBILE_NUM=d.MOBILE_NUM and mayi_info_1.APPLY_TIME=d.APPLY_TIME and mayi_info_1.et between d.et -interval '2' second and d.et
join ad on mayi_info_1.MOBILE_NUM=ad.MOBILE_NUM and mayi_info_1.CERT_NO=ad.CERT_NO and mayi_info_1.APPLY_TIME=ad.APPLY_TIME and mayi_info_1.et between ad.et -interval '2' second and ad.et
join union_data on mayi_info_1.CERT_NO=union_data.id and mayi_info_1.APPLY_TIME=union_data.APPLY_TIME and mayi_info_1.et between union_data.et -interval '2' second and union_data.et
join union_data_phone on mayi_info_1.MOBILE_NUM=union_data_phone.phone and mayi_info_1.APPLY_TIME=union_data_phone.APPLY_TIME and mayi_info_1.et between union_data_phone.et -interval '2' second and union_data_phone.et;













-- =========================================
-- 7天内申请人身份证作为联系人出现次数
create view b as
select
    update_redis(SPOUSE_CERT_NO,count (APP_NO) over w as cnt) as code
FROM mayi_info_1
    window w as (partition by SPOUSE_CERT_NO order by et range between interval '7' DAY preceding and current row );



-- 方法1：写udf,把view b 的结果写到redis中，查询的时候再写一个udf直接从redis中读数据返回结果。
-- 方法2：再创建一个view 求view b 的top1,用这个view 和主流join


-- 7天内申请人手机号作为联系人手机号出现的次数
create view view_c as
select
    SPOUSE_MOBILE_NO,
    count (APP_NO) over w as cnt
FROM mayi_info_1
    window w as (partition by SPOUSE_MOBILE_NO order by et range between interval '7' DAY preceding and current row );
-- =========================================


-- 测试数据

{"CERT_NO":"123","APP_NO":"a","SPOUSE_CERT_NO":"321","MOBILE_NUM":"aaa","COMP_NAME":"aaa","STANDARD_ADDR":"a_b","APPLY_TIME":1}
{"CERT_NO":"789","APP_NO":"b","SPOUSE_CERT_NO":"987","MOBILE_NUM":"aaa","COMP_NAME":"bbb","STANDARD_ADDR":"a_b","APPLY_TIME":2}
{"CERT_NO":"123","APP_NO":"c","SPOUSE_CERT_NO":"321","MOBILE_NUM":"aaa","COMP_NAME":"aaa","STANDARD_ADDR":"a_c","APPLY_TIME":3}
{"CERT_NO":"123","APP_NO":"d","SPOUSE_CERT_NO":"123","MOBILE_NUM":"aaa","COMP_NAME":"bbb","STANDARD_ADDR":"a_c","APPLY_TIME":4}
{"CERT_NO":"321","APP_NO":"e","SPOUSE_CERT_NO":"123","MOBILE_NUM":"aaa","COMP_NAME":"aaa","STANDARD_ADDR":"a_b","APPLY_TIME":5}
{"CERT_NO":"321","APP_NO":"e","SPOUSE_CERT_NO":"123","MOBILE_NUM":"aaa","COMP_NAME":"aaa","STANDARD_ADDR":"a_b","APPLY_TIME":5}
{"CERT_NO":"987","APP_NO":"g","SPOUSE_CERT_NO":"123","MOBILE_NUM":"aaa","COMP_NAME":"aaa","STANDARD_ADDR":"a_b","APPLY_TIME":7}
{"CERT_NO":"321","APP_NO":"a","SPOUSE_CERT_NO":"123","MOBILE_NUM":"aaa","COMP_NAME":"bbb","STANDARD_ADDR":"a_b","APPLY_TIME":8}
{"CERT_NO":"256","APP_NO":"a","SPOUSE_CERT_NO":"444","MOBILE_NUM":"aaa","COMP_NAME":"aaa","STANDARD_ADDR":"a_b","APPLY_TIME":9}
{"CERT_NO":"123","APP_NO":"dda","SPOUSE_CERT_NO":"123","MOBILE_NUM":"aaa","COMP_NAME":"bbb","STANDARD_ADDR":"a_c","APPLY_TIME":10}










