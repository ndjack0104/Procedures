create or replace view dzhc_batchno_hcbakecode_waste as
Select
 ltrim(max(sys_connect_by_path(t.operation_code, ' ')),' ') OperationCode,
t.batchno,t.thous_num,t.point_num,t.bakecode,t.Flag
from
(
Select
t.rnfirst,t.batchno,t.reserve,t.thous_num,t.point_num,t.bakecode,t.operation_code,t.Flag,
lead(t.rnFirst) over(partition by t.bakecode order by rnFirst) as rnNext
from
(
Select
row_number() over (order by t.bakecode) rnFirst,
t.batchno,t.reserve,t.thous_num,t.point_num,t.bakecode,t.operation_code,t.Flag
from
(
Select
case when t.reserve=4 then 'B'
     when t.reserve=10 then 'm'
     when t.reserve=9 and t.ZBFlag=0 then 'z'
     when t.reserve=9 and t.ZBFlag=1 then 'b'
     when t.reserve=11 then 'F'
     else 'W' end as operation_code
     ,t.batchno,t.reserve,t.thous_num,t.point_num,t.bakecode,t.Flag
from
(Select
case when t.photo_ipu=1 then 0 when t.photo_ipu=2 then 0 when t.photo_ipu=3 then 0
     when t.photo_ipu=4 then 0 when t.photo_ipu=5 then 1 when t.photo_ipu=6 then 1
     when t.photo_ipu=7 then 1 when t.photo_ipu=8 then 1 else -1
end as ZBFlag,t.batchno,t.reserve,t.thous_num,t.point_num,t.bakecode,t.Flag
from qa_inspect_batchno_detail t) t
group by t.zbflag,t.batchno,t.reserve,t.thous_num,t.point_num,t.bakecode,t.Flag
) t
) t
) t
start with t.rnNext is null connect by t.rnNext = prior t.rnFirst
group by t.batchno,t.thous_num,t.point_num,t.bakecode,t.Flag;
