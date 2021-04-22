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






create view view1 as
select
    mayi_info_1.CERT_NO,
    a_1.cnt                     as 1days_cert_apply_num,
    union_data_1.cnt            as 1days_cert_contact_num,
    union_data_phone_1.cnt      as 1days_mobile_contact_num,
    a_1.cnt-ad_1.cnt+d_1.cnt    as 1days_cert_or_mobile_num,
    g_1.cnt                     as 1days_cert_couple_num,
    h_1.cnt                     as 1days_cert_couple_mobile_num,
    e_1.cnt                     as 1days_company_apply_num,
    f_1.cnt                     as 1days_address_apply_num,
    a_3.cnt                     as 3days_cert_apply_num,
    union_data_3.cnt            as 3days_cert_contact_num,
    union_data_phone_3.cnt      as 3days_mobile_contact_num,
    a_3.cnt-ad_3.cnt+d_3.cnt    as 3days_cert_or_mobile_num,
    g_3.cnt                     as 3days_cert_couple_num,
    h_3.cnt                     as 3days_cert_couple_mobile_num,
    e_3.cnt                     as 3days_company_apply_num,
    f_3.cnt                     as 3days_address_apply_num,
    a.cnt                     as 7days_cert_apply_num,
    union_data.cnt            as 7days_cert_contact_num,
    union_data_phone.cnt      as 7days_mobile_contact_num,
    a.cnt-ad.cnt+d.cnt        as 7days_cert_or_mobile_num,
    g.cnt                     as 7days_cert_couple_num,
    h.cnt                     as 7days_cert_couple_mobile_num,
    e.cnt                     as 7days_company_apply_num,
    f.cnt                     as 7days_address_apply_num
from mayi_info_1
join a on mayi_info_1.CERT_NO=a.CERT_NO and mayi_info_1.APPLY_TIME=a.APPLY_TIME                                                         and mayi_info_1.et between a.et -interval '2' second and a.et
join e on mayi_info_1.COMP_NAME=e.COMP_NAME and mayi_info_1.APPLY_TIME=e.APPLY_TIME                                                     and mayi_info_1.et between e.et -interval '2' second and e.et
join f on mayi_info_1.STANDARD_ADDR=f.STANDARD_ADDR and mayi_info_1.APPLY_TIME=f.APPLY_TIME                                             and mayi_info_1.et between f.et -interval '2' second and f.et
join d on mayi_info_1.MOBILE_NUM=d.MOBILE_NUM and mayi_info_1.APPLY_TIME=d.APPLY_TIME                                                   and mayi_info_1.et between d.et -interval '2' second and d.et
join ad on mayi_info_1.MOBILE_NUM=ad.MOBILE_NUM and mayi_info_1.CERT_NO=ad.CERT_NO and mayi_info_1.APPLY_TIME=ad.APPLY_TIME             and mayi_info_1.et between ad.et -interval '2' second and ad.et
join union_data on mayi_info_1.CERT_NO=union_data.id and mayi_info_1.APPLY_TIME=union_data.APPLY_TIME                                   and mayi_info_1.et between union_data.et -interval '2' second and union_data.et
join union_data_phone on mayi_info_1.MOBILE_NUM=union_data_phone.phone and mayi_info_1.APPLY_TIME=union_data_phone.APPLY_TIME           and mayi_info_1.et between union_data_phone.et -interval '2' second and union_data_phone.et
join a_1 on mayi_info_1.CERT_NO=a_1.CERT_NO and mayi_info_1.APPLY_TIME=a_1.APPLY_TIME                                                   and mayi_info_1.et between a_1.et -interval '2' second and a_1.et
join e_1 on mayi_info_1.COMP_NAME=e_1.COMP_NAME and mayi_info_1.APPLY_TIME=e_1.APPLY_TIME                                               and mayi_info_1.et between e_1.et -interval '2' second and e_1.et
join f_1 on mayi_info_1.STANDARD_ADDR=f_1.STANDARD_ADDR and mayi_info_1.APPLY_TIME=f_1.APPLY_TIME                                       and mayi_info_1.et between f_1.et -interval '2' second and f_1.et
join d_1 on mayi_info_1.MOBILE_NUM=d_1.MOBILE_NUM and mayi_info_1.APPLY_TIME=d_1.APPLY_TIME                                             and mayi_info_1.et between d_1.et -interval '2' second and d_1.et
join ad_1 on mayi_info_1.MOBILE_NUM=ad_1.MOBILE_NUM and mayi_info_1.CERT_NO=ad_1.CERT_NO and mayi_info_1.APPLY_TIME=ad_1.APPLY_TIME     and mayi_info_1.et between ad_1.et -interval '2' second and ad_1.et
join union_data_1 on mayi_info_1.CERT_NO=union_data_1.id and mayi_info_1.APPLY_TIME=union_data_1.APPLY_TIME                             and mayi_info_1.et between union_data_1.et -interval '2' second and union_data_1.et
join union_data_phone_1 on mayi_info_1.MOBILE_NUM=union_data_phone_1.phone and mayi_info_1.APPLY_TIME=union_data_phone_1.APPLY_TIME     and mayi_info_1.et between union_data_phone_1.et -interval '2' second and union_data_phone_1.et
join a_3 on mayi_info_1.CERT_NO=a_3.CERT_NO and mayi_info_1.APPLY_TIME=a_3.APPLY_TIME                                                   and mayi_info_1.et between a_3.et -interval '2' second and a_3.et
join e_3 on mayi_info_1.COMP_NAME=e_3.COMP_NAME and mayi_info_1.APPLY_TIME=e_3.APPLY_TIME                                               and mayi_info_1.et between e_3.et -interval '2' second and e_3.et
join f_3 on mayi_info_1.STANDARD_ADDR=f_3.STANDARD_ADDR and mayi_info_1.APPLY_TIME=f_3.APPLY_TIME                                       and mayi_info_1.et between f_3.et -interval '2' second and f_3.et
join d_3 on mayi_info_1.MOBILE_NUM=d_3.MOBILE_NUM and mayi_info_1.APPLY_TIME=d_3.APPLY_TIME                                             and mayi_info_1.et between d_3.et -interval '2' second and d_3.et
join ad_3 on mayi_info_1.MOBILE_NUM=ad_3.MOBILE_NUM and mayi_info_1.CERT_NO=ad_3.CERT_NO and mayi_info_1.APPLY_TIME=ad_3.APPLY_TIME     and mayi_info_1.et between ad_3.et -interval '2' second and ad_3.et
join union_data_3 on mayi_info_1.CERT_NO=union_data_3.id and mayi_info_1.APPLY_TIME=union_data_3.APPLY_TIME                             and mayi_info_1.et between union_data_3.et -interval '2' second and union_data_3.et
join union_data_phone_3 on mayi_info_1.MOBILE_NUM=union_data_phone_3.phone and mayi_info_1.APPLY_TIME=union_data_phone_3.APPLY_TIME     and mayi_info_1.et between union_data_phone_3.et -interval '2' second and union_data_phone_3.et
join g on mayi_info_1.APPLY_TIME=g.APPLY_TIME                                                                                           and mayi_info_1.et between g.et -interval '2' second and g.et
join h on mayi_info_1.APPLY_TIME=h.APPLY_TIME                                                                                           and mayi_info_1.et between h.et -interval '2' second and h.et