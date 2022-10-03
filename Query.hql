with min_ts as
(
select user_id
     , session_id
     , min(timestamp) as ts
  from T_clickstream
 where lower(event_type) like "%error%"
 group by user_id
        , session_id

),
pre_tbl as 
(
select t.*
  from T_clickstream as t 
  left join min_ts as m
    on t.user_id = m.user_id
   and t.session_id = m.session_id
 where t.timestamp <= coalesce(m.ts, t.timestamp)
   and lower(event_type) not like "%error%"
  sort by t.user_id
        , t.session_id
        , t.timestamp
 ),
tbl as 
(
select user_id
     , session_id
     , concat_ws('-', COLLECT_LIST(event_page)) as path
  from pre_tbl
 where event_type = "page"
 group by user_id
        , session_id 
)
select path
     , count(*) as cnt
  from tbl
 group by path
 order by cnt desc
 limit 30;
