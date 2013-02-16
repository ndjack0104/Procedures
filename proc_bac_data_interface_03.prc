CREATE OR REPLACE PROCEDURE proc_bac_data_interface_03

is

cursor c1  is
---用游标将表头的数据一条一条取出，取出的数据在传输日志表qa_trans_logs中不存在
---如果需要重新处理数据，需要删除已传输的质量数据以及传输日志表的记录


--attr_0拿来作为一个区分02品和03品的标志字段，已与华夏的人联系过，如果不一致，在此处修改
Select item_id,operation_id,machine_id,batchno,qa_start_date,qa_end_date,
       CHECK_QUANTITY,WASTE_QUANTITY
from QA_BAC_BATCHNO
where not exists(select 1 from
          qa_trans_logs where cart_number=QA_BAC_BATCHNO.BATCHNO and qa_start_date=QA_BAC_BATCHNO.qa_start_date and OPERATION_ID=3 AND TRANS_TYPE=1) and attr_0 = 3 ;
--定义变量，存放游标取出的数据
v_item_id QA_BAC_BATCHNO.item_id%type;
v_operation_id QA_BAC_BATCHNO.operation_id%type;
v_machine_id QA_BAC_BATCHNO.machine_id%type;
v_batchno QA_BAC_BATCHNO.batchno%type;
v_qa_start_date QA_BAC_BATCHNO.qa_start_date%type;
v_qa_end_date QA_BAC_BATCHNO.qa_end_date%type;
v_CHECK_QUANTITY QA_BAC_BATCHNO.CHECK_QUANTITY%type;
v_WASTE_QUANTITY QA_BAC_BATCHNO.WASTE_QUANTITY%type;
v_job_id wip_jobs.job_id%type;
v_log_id wip_prod_logs.log_id%type;
zv_machine_id wip_prod_logs.machine_id%type;
cnt number;
ret number;
v_row number;
waster_total number;
--gzName VARCHAR2(20);
--gzPrnCartNumber VARCHAR2(20);
--inpsect_master_inspectm_id number;

begin
--打开游标并循环
 open c1;
loop
--取出游标中的数据存到变量中
  fetch c1 into v_item_id,v_operation_id,v_machine_id,v_batchno,v_qa_start_date,v_qa_end_date,v_CHECK_QUANTITY,v_WASTE_QUANTITY;
  exit when c1%notfound;
--如果车号与时间重复则返回
   select count(*) into v_row from QA_BAC_BATCHNO where batchno=v_batchno and qa_start_date=v_qa_start_date;
   if v_row=1 then
  --两边数据库的机器内码不一致，对应关系如下
   if v_machine_id='51DZJ02' then
       zv_machine_id:=7;
     else
       zv_machine_id:=8;
    end if  ;
    --判断工单表中是否有这一车的记录，如果没有就返回
    --此处可能造成与票面信息不同步
    select count(*) into ret from wip_jobs where cart_number=v_batchno;
    if ret =1 then
      Select job_id into v_job_id  from wip_jobs where cart_number=v_batchno;
      Select count(*) into waster_total from QA_BAC_BATCHNO_DETAIL
             where  ITEM_ID= v_item_id and operation_id=v_operation_id and machine_id=v_machine_id and batchno=v_batchno
             and qa_date between v_qa_start_date and v_qa_end_date;

     if v_job_id>0 and waster_total=v_WASTE_QUANTITY then
        --判断如果表头的作废数量是否与表体的记录数相同，如果相同可以认为凹印数据上传完毕，可以转换
       Select count(*) into cnt from wip_prod_logs where job_id=v_job_id and operation_id=9 and start_date=v_qa_start_date;
       --判断生产日志中是否有记录，如果没有就插入一条
       --识标记录也会产生生产日志，这里为了避免重复
        if cnt<1 then
            insert into wip_prod_logs(log_id,job_id,operation_id,machine_id,work_unit_id,operator_id,start_date,end_date,item_flag)
             values(sq_wip_prod_logs.nextval,v_job_id,9,zv_machine_id,0,0,v_qa_start_date,v_qa_end_date,1);
         end if;
        --取得LOG_ID，考虑到甩车的情况，要加上日期start_date
        Select log_id into v_log_id from   wip_prod_logs where job_id=v_job_id and operation_id=9 and start_date=v_qa_start_date;
        --写入质量主表记录
      --设置 item_flag=3 表示印码三合一已判废
      delete from qa_inspect_master where qa_inspect_master.log_id=v_log_id;
      insert into qa_inspect_master(inspectm_id,job_id,log_id,operation_id,operator_id,
             image_path,machine_waster_number,image_flag,item_flag,info_number)
              values(sq_qa_inspect_master.nextval,v_job_id,v_log_id,9,0,
              '',v_WASTE_QUANTITY,2,3,v_CHECK_QUANTITY);

        --写入质量从表记录

     insert into qa_inspect_slave(inspects_id,inspectm_id,sheet_id,sheet_num,
     convert_sheet_number,route_sheet_number, error_grade,file_index,file_name,item_flag,error_id,p_Type,p_Left,p_Up,p_Right,p_Down,EX_HEAD,EX_CODE)
           --2011-08-30 mask
    -- Select sq_qa_inspect_slave.nextval,sq_qa_inspect_master.currval,SHEET_ID,substr(SHEET_CODE,4,9),mod(POINT_NUM - 1,8)+1,
    Select sq_qa_inspect_slave.nextval,sq_qa_inspect_master.currval,sheet_id,substr(SHEET_CODE,2,2)||ltrim(substr(SHEET_CODE,4,7),0),mod(POINT_NUM - 1,8)+1,
    trunc((POINT_NUM-1)/8)+1,0,file_index,file_name,(check_flag+1),WASTE_TYPEID,p_Type,p_Left,p_Up,p_Right,p_Down,substr(BAKECODE,1,2),substr(BAKECODE,3,8)
             from QA_BAC_BATCHNO_DETAIL
             where ITEM_ID= v_item_id and operation_id=v_operation_id and machine_id=v_machine_id and batchno=v_batchno
              and qa_date between v_qa_start_date and v_qa_end_date;

        --根据作废类型对应关系表 ch_bas_waste_def更新作废类型
     UPDATE qa_inspect_slave SET error_id=(select ch_bas_waste_def.MATCH_TYPE_ID
     from ch_bas_waste_def
            where qa_inspect_slave.error_id=ch_bas_waste_def.waster_type_id )
             where  exists(select 1 from qa_inspect_master where
             inspectm_id=qa_inspect_slave.inspectm_id and log_id=v_log_id);
      insert into qa_trans_logs  (cart_number,qa_start_date ,waste_quantity,OPERATION_ID,TRANS_TYPE)
             values(v_batchno,sysdate,v_WASTE_QUANTITY,3,1);
      end if;
     end if;
   end if ;
 end loop
  commit;


end proc_bac_data_interface_03;
/
