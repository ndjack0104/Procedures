create or replace package DZHC_Interface_pkg as

  procedure    AcceptDZHCBatchNo(batchnoStr varchar2,OperationID varchar2,MachineCode varchar2,UserCode varchar2,RoleFlag number,BackInt out number);
  procedure    SaveDZHC_BuLuBatchNo(batchnoStr varchar2,OperationID varchar2,BackInt out number);
  procedure    Transfer_dzhc_To_xzhc(batchnoStr varchar2);


end DZHC_Interface_pkg;
/
create or replace package body DZHC_Interface_pkg as

  procedure  SaveDZHC_BuLuBatchNo(batchnoStr varchar2,OperationID varchar2,BackInt out number) is
    SavePt1    number;
    SavePt2    number;
  begin
    BackInt := 0;
    update qa_inspect_batchno t set t.flag = 3 where t.batchno=batchnoStr;
    Update qa_inspect_batchno t set t.flag = 3 ,t.err_quantity = (Select Count(1) from qa_inspect_batchno_detail
          where batchno=batchnoStr and Flag=3),t.waste_quantity=(Select Count(1) from qa_inspect_batchno_detail
          where batchno=batchnoStr and Flag=3) where t.batchno=batchnoStr and t.operation_id=OperationID;
    SavePt1 := sql%rowcount;
    Update qa_action_log t set t.e_date = sysdate where t.batchno=batchnoStr and t.act=OperationID;
    SavePt2 := sql%rowcount;
    if ((SavePt1=0) or (SavePt2=0)) then
      RAISE_APPLICATION_ERROR(-20100,'数据异常,未保存成功,qa_inspect_batchno,qa_action_log'||to_char(SavePt1,'99')||','||to_char(SavePt2,'99'));
    end if;
    BackInt := 1;

    exception when others then
      rollback ;
      BackInt := 0;
      raise;
  end;
      
  procedure  AcceptDZHCBatchNo(batchnoStr varchar2,OperationID varchar2,MachineCode varchar2,UserCode varchar2,RoleFlag number,BackInt out number) is
    CartStr     qa_inspect_batchno.batchno%type;
    NowDate     date;                --RoleFlag = 0 操作员  =1 管理员
    TmpInt      number;
    OpID        number;
    ItemFlag    number;
  begin
      TmpInt:=0;
      Select sysdate into NowDate from dual;
      Select Count(1) into TmpInt from qa_inspect_batchno where batchno=batchnoStr and Operation_Id=OperationID;
      if (TmpInt > 0) then
         Select last_update_by,Flag into OpID,ItemFlag from qa_inspect_batchno where batchno=batchnoStr and Operation_Id=OperationID;
         if ((ItemFlag=1) and (OpID=UserCode)) then
           BackInt := 1;
           return;
         elsif ((ItemFlag=1) and (OpID<>UserCode)) then
           RAISE_APPLICATION_ERROR(-20101,'其他操作员正在操作中');
         elsif ((ItemFlag=3) and (RoleFlag=0)) then
           RAISE_APPLICATION_ERROR(-20100,'操作员权限不能重复操作该车数据');
         elsif (ItemFlag=5) then
           RAISE_APPLICATION_ERROR(-20102,'剔废报表已经打印，数据不允许修改');             
         elsif (RoleFlag=0) then
           RAISE_APPLICATION_ERROR(-20100,'非法参数Flag,Role,'||to_char(ItemFlag,'99')||','||to_char(RoleFlag,'9'));
         end if;
      end if;
      
      Select batchno into CartStr from qa_inspect_batchno where batchno=batchnoStr and (flag=3 or flag=2) and rownum<2 for update nowait;
      if (TmpInt = 0) then
        insert into qa_inspect_batchno (item_id, operation_id, machine_id, batchno, check_date, emp_id, waste_quantity,
        err_quantity, flag, last_update_by, last_update_date,created_date)
        (Select item_id,OperationID, MachineCode, batchno, NowDate, UserCode,
        waste_quantity, err_quantity, 3,UserCode, NowDate,created_date from qa_inspect_batchno where batchno=batchnoStr and rownum=1);
      end if;
      Update qa_inspect_batchno t set t.flag = 1,t.last_update_by=UserCode,t.last_update_date=NowDate where batchno=batchnoStr and flag=3;
      insert into qa_action_log (batchno,emp_id,a_date,act) values (CartStr,UserCode,NowDate,OperationID);

      BackInt := 1;
      commit;
    exception when others then
      rollback ;
      BackInt := 0;
      raise;
  end;

  procedure  Transfer_dzhc_To_xzhc(batchnoStr varchar2)  is

    TmpInt number;
  begin
    Select Count(1) into TmpInt from qa_inspect_batchno t where t.batchno=batchnoStr and (t.FLAG=1 or t.FLAG=5);
    if (TmpInt > 0) then
      RAISE_APPLICATION_ERROR(-20101,'该万的当前状态不允许进行导入操作');
    end if;
    delete from qa_inspect_batchno t where t.batchno = batchnoStr;
    delete from qa_inspect_batchno_detail t where t.batchno = batchnoStr;
    delete from qa_inspect_uncheck t where t.batchno = batchnoStr;
    delete from qa_action_log t where t.batchno = batchnoStr;
    delete from qa_bakecode t where t.batchno = batchnoStr and t.point_num = 1 ;

    Insert Into qa_inspect_batchno
      (item_id,
       machine_id,
       operation_id,
       batchno,
       created_date,
       emp_id,
       waste_quantity,
       err_quantity,
       flag)
     Select 
             distinct
             '0103.9602',
             case when z.operation_id=2 then '41DMJ07'
                  when z.operation_id=0 then '21XSJ02-D'
             else '未知' end as machine_id,                               
             case when z.operation_id=2 then '2110'
                  when z.operation_id=0 then '5110'
             else '未知' end as operation_id,             
             z.cart_number,
             sysdate,
             '1',
             0,
             10000,
             3
        from zxjc.qa_dtbl z             
       where (z.operation_id = 0 or z.operation_id = 2)        
         and z.cart_number = batchnoStr;

    Insert Into qa_inspect_batchno_detail
      (seq_id,
       batchno,
       operation_id,
       machine_id,
       sheet_id,
       sheet_code,
       thous_num,
       point_num,
       bakecode,
       waste_typeid,
       waste_rate,
       flag,
       reserve,
       photo_ipu)
      select inspect_sequence.NEXTVAL,
             cart_number,
             (select e.OPERATION_CODE
                from zxjc.wip_jobs a,
                     (Select operation_id, max(machine_id) as machine_id, job_id
                        from zxjc.wip_prod_logs
                       group by job_id, operation_id) b,
                     zxjc.dic_machines c,
                     zxjc.qa_dtbl d,
                     (Select machine_code, operation_code
                        from ch_bas_machine_plan
                       group by machine_code, operation_code) e
               where d.cart_number = a.cart_number
                 and a.job_id = b.job_id
                 and d.operation_id = b.operation_id
                 and c.machine_id = b.machine_id
                 and e.MACHINE_CODE = c.note
                 and d.id = t.id),
             (select e.machine_code
                from zxjc.wip_jobs a,
                     (Select operation_id, max(machine_id) as machine_id, job_id
                        from zxjc.wip_prod_logs
                       group by job_id, operation_id) b,
                     zxjc.dic_machines c,
                     zxjc.qa_dtbl d,
                     (Select machine_code, operation_code
                        from ch_bas_machine_plan
                       group by machine_code, operation_code) e
               where d.cart_number = a.cart_number
                 and a.job_id = b.job_id
                 and d.operation_id = b.operation_id
                 and c.machine_id = b.machine_id
                 and e.MACHINE_CODE = c.note
                 and d.id = t.id),
             (select sheet_id
                from zxjc.qa_inspect_slave
               where inspects_id = t.inspects_id),
             substr(sheet_num, 1, 10),
             thousand_index,
             small_paper_num,
             substr(code_num, 1, 10),
             (select error_id
                from zxjc.qa_inspect_slave
               where inspects_id = t.inspects_id),
             (select error_grade
                from zxjc.qa_inspect_slave
               where inspects_id = t.inspects_id),
             1,
             case
             when operation_id = 0 then
             '4'
             when operation_id = 2 then
             '9'
             when operation_id = 9 then
             '10'
             else '4'
             end,
             case
             when p_type = 0 then
             '1'
             when p_type = 1 then
             '5'
             end
        from zxjc.qa_dtbl t
       where CART_NUMBER = batchnoStr
         and t.operation_id <> 9;

    Insert Into qa_inspect_batchno_detail
      (seq_id,
       batchno,
       operation_id,
       machine_id,
       sheet_id,
       sheet_code,
       thous_num,
       point_num,
       bakecode,
       waste_typeid,
       waste_rate,
       flag,
       RESERVE,
       PHOTO_IPU)
      select inspect_sequence.NEXTVAL,
             batchno,
             '印码',
             machine_id,
             sheet_id,
             substr(sheet_code, 4, 10),
             case
             when 
             floor(substr(bakecode,7,1)) = 9
             then 9
             else
             mod(floor((substr(bakecode, 7, 4)+1)/1000),10)
             end,
             point_num,
             substr(bakecode, 1, 1) || substr(bakecode, 3, 1) || substr(bakecode,2,1) || substr(bakecode, 4, 10),
             waste_typeid,
             waste_rate,
             check_flag,
             '10',
             '1'
        from zxjc.qa_bac_batchno_detail
       where batchno = batchnoStr
         and check_flag = 1;

    insert Into qa_inspect_uncheck
      (MACHINE_ID,
       BATCHNO,
       THOUS_NUM,
       UNCHECK_TYPE,
       CREATED_DATE,
       HUNDRED_NUM,
       BAKECODE,
       OPERATION_ID,
       QA_DATE,
       sheet_code)
      select (Select t.machine_id
                from zxjc.qa_mbac_batchno t
               where t.batchno = z.cart_number),
             z.cart_number,
             z.thousand_index,
             z.item_flag,
             sysdate,
             z.hundred_index,
             z.code_num,
             case
               when max(z.operation_id) = 2 then
                '印码'
               when max(z.operation_id) = 4 then
                '印码'
               when max(z.operation_id) = 0 then
                '胶一印印刷'
               else
                '未知工序'
             end as OPERATION_CODE,
             sysdate,
             max(z.sheet_num)
        from zxjc.qa_ncktab z
       where CART_NUMBER = batchnoStr
       group by z.cart_number,
                z.thousand_index,
                z.item_flag,
                z.hundred_index,
                z.code_num;
      
      
insert into qa_bakecode
  (ITEM_ID,
   OPERATION_ID,
   MACHINE_ID,
   batchno,
   point_num,
   bakecode,
   created_date)

  select 0103.9602,
         2,
         machine_id,
         batchno,
         1,
         substr(bakecode, 1, 1) || substr(bakecode, 3, 1) ||
         substr(bakecode, 2, 1) || substr(bakecode, 4,3)||'0000',
         sysdate
    from zxjc.QA_REC_BATCHNO where batchno =  batchnoStr;
      
      
      
      commit;



      exception when others then
        rollback;
        raise;



  end;







end DZHC_Interface_pkg;
/
