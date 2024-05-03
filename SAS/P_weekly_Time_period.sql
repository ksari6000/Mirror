procedure        P_WKLY_TIME_PERIOD                                             
(num_weeks in number) Is                                                        
                                                                                
/*Step 1 */                                                                     
                                                                                
                                                                                
P_Process_Id Number;                                                            
                                                                                
Jobno Binary_Integer;                                                           
                                                                                
Parm_Start_Week Number;                                                         
                                                                                
Parm_End_Week Number;                                                           

                                                                                
Begin                                                                           
                                                                                
                                                                                
/* Clear Table to Insert New Weeks*/                                            
Execute Immediate 'Truncate Table wkly_sas_prod_time_pd';                       
                                                                                
Update Sas_Process_Sw                                                           
                                                                                
Set Process_Time = Sysdate, Process_Complete = 'false';                         
Commit;                                                                         
                                                                                
                                                                                

Insert Into Sas_Process_Log_Id (Process_Ind,Process_Date,Process_Type)          
Values ('I',Sysdate,'WEEKLY');                                                  
Commit;                                                                         
                                                                                
                                                                                
                                                                                
/* Get Process_id from Log */                                                   
Select                                                                          
                                                                                
  Max(Process_Id) Into P_Process_Id                                             
From                                                                            
                                                                                
  Sas_Process_Log_Id                                                            

                                                                                
where Process_Type = 'WEEKLY';                                                  
                                                                                
/*Write to Log*/                                                                
                                                                                
Insert Into Sas_Process_Log                                                     
    (Process_Id, Process_Step, Process_Name,                                    
     Process_Start_Time, Process_Table, Process_Ind)                            
Values                                                                          
                                                                                
    (P_Process_Id, 10, 'Step 1:Get weeks to process:p_wkly_time_period ',       
     Sysdate, 'Wkly_Sas_Prod_Time_Pd', 'I');                                    
Commit;                                                                         

                                                                                
                                                                                
                                                                                
                                                                                
/*Process/Insert Week*/                                                         
                                                                                
For Rec In                                                                      
                                                                                
 (Select Mc.Merchandising_Year, Mc.Merchandising_Week, Mc.Merchandising_Period  
   From (Select * From Merchandising_Calendars                                  
         Where                                                                  
                                                                                
           Business_Unit_Id = 30 And Week_Ending_Date <= Trunc(Sysdate) - 1     

         Order By                                                               
                                                                                
           Merchandising_Year Desc, Merchandising_Week Desc) Mc                 
   Where Rownum <= Nvl(Num_Weeks,1))                                            
   --Where Rownum <= 6)                                                         
                                                                                
Loop                                                                            
                                                                                
  Insert Into Wkly_Sas_Prod_Time_Pd                                             
  Select rec.Merchandising_Year, rec.Merchandising_Week From Dual;              
  Commit;                                                                       
                                                                                
End Loop;                                                                       

                                                                                
                                                                                
/*Get Ending week */                                                            
                                                                                
Select Merch_Week Into Parm_End_Week                                            
From (Select * From Wkly_Sas_Prod_Time_Pd                                       
      Order By Merch_Year Desc, Merch_Week Desc) Where Rownum = 1;              
                                                                                
/*Get Starting Week*/                                                           
                                                                                
Select Merch_Week Into Parm_Start_Week                                          
From (Select * From Wkly_Sas_Prod_Time_Pd                                       
      Order By Merch_Year Asc, Merch_Week Asc) Where Rownum = 1;                

                                                                                
                                                                                
Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate,       
process_name = 'Step 1:Get weeks to process:p_wkly_time_period: '||Parm_Start_Week|| 
' thru ' ||Parm_End_Week                                                                                          
Where Process_Step = 10 And Process_Id = P_Process_Id;                          
commit;                                                                         
                                                                                
--TODO ASK LARRY                                                                                
Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate        
Where Process_Step = 10 And Process_Id = P_Process_Id;                          
commit;                                                                         

                                                                                
                                                                                
Dbms_Job.Submit(Jobno, 'P_Wkly_Inventory_Extract();', Sysdate, Null);           
commit;                                                                         
                                                                                
                                                                                
END P_WKLY_TIME_PERIOD;                                                         

109 rows selected.

SQL> spool off
