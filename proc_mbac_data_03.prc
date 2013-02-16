CREATE OR REPLACE PROCEDURE proc_mbac_data_03

is

cursor c1  is
---���α꽫��ͷ������һ��һ��ȡ����ȡ���������ڴ�����־��qa_trans_logs�в�����
---�����Ҫ���´������ݣ���Ҫɾ���Ѵ�������������Լ�������־��ļ�¼

--attr_0������Ϊһ������02Ʒ��03Ʒ�ı�־�ֶΣ����뻪�ĵ�����ϵ���������һ�£��ڴ˴��޸�
Select item_id,operation_id,machine_id,batchno,qa_start_date,qa_end_date,
       CHECK_QUANTITY,ERROR_QUANTITY
from QA_MBAC_BATCHNO
where not exists(select 1 from
          qa_trans_logs where cart_number=QA_MBAC_BATCHNO.BATCHNO and qa_start_date=QA_MBAC_BATCHNO.qa_start_date and OPERATION_ID=2 AND TRANS_TYPE=1) and attr_0 = 3 ;
--�������������α�ȡ��������
v_item_id QA_MBAC_BATCHNO.item_id%type;
v_operation_id QA_MBAC_BATCHNO.operation_id%type;
v_machine_id QA_MBAC_BATCHNO.machine_id%type;
v_batchno QA_MBAC_BATCHNO.batchno%type;
v_qa_start_date QA_MBAC_BATCHNO.qa_start_date%type;
v_qa_end_date QA_MBAC_BATCHNO.qa_end_date%type;
v_CHECK_QUANTITY QA_MBAC_BATCHNO.CHECK_QUANTITY%type;
v_WASTE_QUANTITY QA_MBAC_BATCHNO.ERROR_QUANTITY%type;
v_job_id wip_jobs.job_id%type;
v_log_id wip_prod_logs.log_id%type;
zv_machine_id wip_prod_logs.machine_id%type;
cnt number;
ret number;
v_row number;
waster_total number;
gzName VARCHAR2(20);
gzPrnCartNumber VARCHAR2(20);
--inpsect_master_inspectm_id number;

begin
--���α겢ѭ��
 open c1;
loop
--ȡ���α��е����ݴ浽������
  fetch c1 into v_item_id,v_operation_id,v_machine_id,v_batchno,v_qa_start_date,v_qa_end_date,v_CHECK_QUANTITY,v_WASTE_QUANTITY;
  exit when c1%notfound;
--���������ʱ���ظ��򷵻�
   select count(*) into v_row from QA_MBAC_BATCHNO where batchno=v_batchno and qa_start_date=v_qa_start_date;
   if v_row=1 then
  --�������ݿ�Ļ������벻һ�£���Ӧ��ϵ����
   if v_machine_id='51DZJ02' then
       zv_machine_id:=7;
     else
       zv_machine_id:=8;
    end if  ;
    --�жϹ��������Ƿ�����һ���ļ�¼������оͷ���
    --�˴���02Ʒ��ͬ��03Ʒ�Ľ�ӡδ�ϴ���wip_jobs�в����иó���¼�����ڴ˹�������ǰ���г��μ�¼�����������
    select count(*) into ret from wip_jobs where cart_number=v_batchno;
    if ret =0 then
      insert into wip_jobs(job_id,cart_number,product_id,prod_quantity,start_date,released_flag,finished_flag)
             values(SQ_WIP_JOBS.nextval,v_batchno,3,10000,sysdate,0,0);
    
      Select job_id into v_job_id  from wip_jobs where cart_number=v_batchno;
      Select count(*) into waster_total from QA_MBAC_BATCHNO_DETAIL
             where  ITEM_ID= v_item_id and operation_id=v_operation_id and machine_id=v_machine_id and batchno=v_batchno
             and qa_date between v_qa_start_date and v_qa_end_date;

     if v_job_id>0 and waster_total=v_WASTE_QUANTITY then
        --�ж������ͷ�����������Ƿ������ļ�¼����ͬ�������ͬ������Ϊ��ӡ�����ϴ���ϣ�����ת��
       Select count(*) into cnt from wip_prod_logs where job_id=v_job_id and operation_id=2 and start_date=v_qa_start_date;
       --�ж�������־���Ƿ��м�¼�����û�оͲ���һ��
       --ʶ���¼Ҳ�����������־������Ϊ�˱����ظ�
        if cnt<1 then
            insert into wip_prod_logs(log_id,job_id,operation_id,machine_id,work_unit_id,operator_id,start_date,end_date,item_flag)
             values(sq_wip_prod_logs.nextval,v_job_id,2,zv_machine_id,0,0,v_qa_start_date,v_qa_end_date,1);
         end if;
        --ȡ��LOG_ID�����ǵ�˦���������Ҫ��������start_date
        Select log_id into v_log_id from   wip_prod_logs where job_id=v_job_id and operation_id=2 and start_date=v_qa_start_date;
        --д�����������¼

      delete from qa_inspect_master where qa_inspect_master.log_id=v_log_id;
      insert into qa_inspect_master(inspectm_id,job_id,log_id,operation_id,operator_id,
             image_path,machine_waster_number,image_flag,item_flag,info_number)
              values(sq_qa_inspect_master.nextval,v_job_id,v_log_id,2,0,
              '',v_WASTE_QUANTITY,2,1,v_CHECK_QUANTITY);

        --д�������ӱ��¼

     insert into qa_inspect_slave(inspects_id,inspectm_id,sheet_id,sheet_num,
     convert_sheet_number,route_sheet_number, error_grade,file_index,file_name,item_flag,error_id,p_Type,p_Left,p_Up,p_Right,p_Down)
           --2011-08-30 mask
    -- Select sq_qa_inspect_slave.nextval,sq_qa_inspect_master.currval,SHEET_ID,substr(SHEET_CODE,4,9),mod(POINT_NUM - 1,8)+1,
    Select sq_qa_inspect_slave.nextval,sq_qa_inspect_master.currval,SHEET_ID,substr(SHEET_CODE,2,2)||ltrim(substr(SHEET_CODE,4,7),0),mod(POINT_NUM - 1,8)+1,
    trunc((POINT_NUM-1)/8)+1,0,file_index,file_name,0,WASTE_TYPEID,p_Type,p_Left,p_Up,p_Right,p_Down
             from QA_MBAC_BATCHNO_DETAIL
             where ITEM_ID= v_item_id and operation_id=v_operation_id and machine_id=v_machine_id and batchno=v_batchno
              and qa_date between v_qa_start_date and v_qa_end_date;

        --�����������Ͷ�Ӧ��ϵ�� ch_bas_waste_def������������
     UPDATE qa_inspect_slave SET error_id=(select ch_bas_waste_def.MATCH_TYPE_ID
     from ch_bas_waste_def
            where qa_inspect_slave.error_id=ch_bas_waste_def.waster_type_id )
             where  exists(select 1 from qa_inspect_master where
             inspectm_id=qa_inspect_slave.inspectm_id and log_id=v_log_id);

   --д������δ���¼(Ʊ��)
  select count(*) into ret from QA_MBAC_UNCHECK_DETAIL where ITEM_ID= v_item_id and operation_id=v_operation_id and machine_id=v_machine_id and batchno=v_batchno;
  if ret > 0 then
     insert into qa_inspectstate_slave(inspectst_id,inspectm_id,sheet_id,sheet_num,item_flag,err_flag)
            select sq_qa_inspectstate_slave.nextval,sq_qa_inspect_master.currval, SHEET_ID,BAKECODE,1,0
            from QA_MBAC_UNCHECK_DETAIL
            where   ITEM_ID= v_item_id and operation_id=v_operation_id and machine_id=v_machine_id and batchno=v_batchno;
   end if;
  -- UPDATE qa_inspectstate_slave SET  item_flag=1,err_flag=0  WHERE inspectm_id = sq_qa_inspect_master.currval;
   --д������δ���¼(ʶ��)
  select count(*) into ret from QA_BAC_UNCHECK_DETAIL;-- where ITEM_ID= v_item_id and operation_id=v_operation_id and machine_id=v_machine_id and batchno=v_batchno;
  if ret > 0 then
     insert into qa_inspectstate_slave(inspectst_id,inspectm_id,sheet_id,sheet_num,item_flag,err_flag)
            select sq_qa_inspectstate_slave.nextval,sq_qa_inspect_master.currval, SHEET_ID,BAKECODE,2,0
            from QA_BAC_UNCHECK_DETAIL
            where   ITEM_ID= v_item_id and operation_id=v_operation_id and machine_id=v_machine_id and batchno=v_batchno;
   end if;
  -- UPDATE qa_inspectstate_slave SET  item_flag=2,err_flag=0 WHERE inspectm_id=sq_qa_inspect_master.currval;

-------------------------------
--д��ʶ�������¼
    delete from qa_rectify_master where qa_rectify_master.log_id=v_log_id;
    select count(*) into ret from QA_REC_BATCHNO where BATCHNO=v_batchno;
    --2011-09-20�ڻ�����QA_REC_BATCHNO��һ���ֶ�ATTR_0����ӡ�복��д��ATTR_0
    --select ATTR_0 into gzPrnCartNumber from QA_REC_BATCHNO where BATCHNO=v_batchno;

    --ֱ�Ӵӻ���qa_mbac_batchno_detail��ȡATTR_0
    select max(ATTR_0) into gzPrnCartNumber from qa_mbac_batchno_detail where BATCHNO=v_batchno;

    if ret > 0 then
        select substr(BAKECODE,1,1)||substr(BAKECODE,3,1)|| substr(BAKECODE,2,1)||substr(BAKECODE,4,3) into gzName from QA_REC_BATCHNO where BATCHNO=v_batchno;
        select R_QUANTITY into ret from QA_REC_BATCHNO where BATCHNO=v_batchno;
        insert into qa_rectify_master(RECTIFYM_ID,job_id,log_id,TOTAL_NUMBER,HEAD,ITEM_FLAG,CODE_CARTNUMBER)
        values(sq_qa_rectify_master.nextval,v_job_id,v_log_id,ret,gzName,0,gzPrnCartNumber);
        --д��ʶ�������¼

        insert into qa_rectify_slave(RECTIFYS_ID,RECTIFYM_ID,sheet_id,sheet_num,HEAD,Err_Flag)
        --2011-08-30 mask
        --Select sq_qa_rectify_slave.nextval,sq_qa_rectify_master.currval,SHEET_ID,substr(substr('000000000'||SHEET_CODE,-9),1,9),substr(BAKECODE,2,1)||substr(BAKECODE,4,8),0
       Select sq_qa_rectify_slave.nextval,sq_qa_rectify_master.currval,SHEET_ID,substr(substr('000000000'||SHEET_CODE,-9),1,2)||ltrim(substr(substr(substr('000000000'||SHEET_CODE,-9),1,9),3,7),0),substr(BAKECODE, 3, 8),0

     from QA_BAKECODE_SHEET
        where ITEM_ID= v_item_id and operation_id=v_operation_id and machine_id=v_machine_id and batchno=v_batchno;
  end if;
-------------------------------


   --��������д����ɺ󣬽������Լ�����д�뵽ת����־����
      insert into qa_trans_logs  (cart_number,qa_start_date ,waste_quantity,OPERATION_ID,TRANS_TYPE)
             values(v_batchno,v_qa_start_date,v_WASTE_QUANTITY,2,1);
      end if;
     end if;
   end if ;
 end loop

  commit;


end proc_mbac_data_03;
/
