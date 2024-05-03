procedure        P_WKLY_INVENTORY_EXTRACT                                       
Is                                                                              
                                                                                
/*Step 2 */                                                                     
                                                                            
P_Process_Id Number;                                                                                                                                                                                                                      
Parm_Start_Week Number;                                                                                                                                      
Parm_Start_Year Number;                                                                                                                                                                                                                 
Parm_End_Week Number;                                                                                                                                       
Parm_End_Year Number;                                                                                                                                                                                                                   
Jobno Binary_Integer;                                                                                                                                                                                                                     
V_Where  Varchar2(500);                                                         

Begin                                                                           
                                                                                                                                                            
Select Max(Process_Id) Into P_Process_Id                                        
From Sas_Process_Log_Id Where Process_Type = 'WEEKLY';                          
                                                                                
/*Log Process*/                                                                 
                                                                                
Insert Into Sas_Process_Log                                                     
(Process_Id, Process_Step, Process_Name,                                        
 Process_Start_Time,Process_Table,Process_Ind)                                  
Values                                                                          

                                                                                
(P_Process_Id, 20,'Step 2:Extract Data from MC2P Inventory_Movements:p_wkly_inve
ntory_extract',                                                                 
                                                                                
 Sysdate,'wkly_inv_move_extract', 'I');                                         
Commit;                                                                         
                                                                                
                                                                                
Execute Immediate 'truncate table wkly_inv_move_extract';                       
                                                                                
/*Get Ending week */                                                            
                                                                                
Select                                                                          

                                                                                
  Merch_Week, Merch_Year Into Parm_End_Week, Parm_End_Year                      
From (Select * From Wkly_Sas_Prod_Time_Pd                                       
      Order By Merch_Year Desc, Merch_Week Desc) Where Rownum = 1;              
                                                                                
/*Get Starting Week*/                                                           
                                                                                
Select                                                                          
                                                                                
  Merch_Week, Merch_Year Into Parm_Start_Week, Parm_Start_Year                  
From (Select * From Wkly_Sas_Prod_Time_Pd                                       
      Order By Merch_Year Asc, Merch_Week Asc) Where Rownum = 1;                
                                                                                

                                                                                
/* Extract Data From Inventory Movement@MC2P */                                 
-- Insert /*+ Append */ Into Wkly_Inv_Move_Extract                              
-- Select /*+ FULL(I) PARALLEL(I,8) */                                          
--       I.Site_Id, I.Style_Id, I.Color_Id, I.Size_Id, I.Dimension_Id,          
--       I.Inven_Move_Type, I.Inven_Move_Qty, I.Inven_Move_Date,                
--       I.Retail_Price, 0 Retail_Price_Final, I.Landed_Unit_Cost, I.Average_Cos
t,                                                                              
                                                                                
--       i.Merchandising_Year, i.Merchandising_Week, I.section_id               
-- From   (Inventory_Movements@Mc2p) I                                          
                                                                                
                                                                                

                                                                                
If Parm_Start_Year = Parm_End_Year                                              
Then                                                                            
                                                                                
V_Where := ' Where                                                              
                                                                                
    I.Merchandising_Year = ' ||Parm_Start_Year|| ' and                          
    I.Merchandising_Week Between '||Parm_Start_Week||' And '|| Parm_End_Week;   
                                                                                
execute immediate 'Insert /*+ Append */ Into Wkly_Inv_Move_Extract              
      Select /*+ FULL(I) PARALLEL(I,8) */                                       
       I.Site_Id, I.Style_Id, I.Color_Id, I.Size_Id, I.Dimension_Id,            
       I.Inven_Move_Type, I.Inven_Move_Qty, I.Inven_Move_Date,                  

       I.Retail_Price, 0 Retail_Price_Final, I.Landed_Unit_Cost, I.Average_Cost,
                                                                                
                                                                                
       i.Merchandising_Year, i.Merchandising_Week, I.section_id                 
 From   (Inventory_Movements@Mc2r) I ' || v_where;                              
 Commit;                                                                        
                                                                                
Else                                                                            
                                                                                
--V_Where := ' Where                                                            
                                                                                
--  (I.Merchandising_Year = ' ||Parm_Start_Year ||' And                         
--   I.Merchandising_Week In (Select W.Merch_Week From Wkly_Sas_Prod_Time_Pd W  

--                            Where W.Merch_Year = ' || Parm_Start_Year ||')) Or
                                                                                
                                                                                
--  (I.Merchandising_Year = '||Parm_End_Year||' And                             
--   I.Merchandising_Week In (Select W.Merch_Week From Wkly_Sas_Prod_Time_Pd W  
--                            Where W.Merch_Year = '||Parm_End_Year||'))';      
                                                                                
For Rec In (Select Distinct Merch_Year From Wkly_Sas_Prod_Time_Pd)              
Loop                                                                            
                                                                                
   For Rec2 In (Select min(Merch_Week) min_week, max(Merch_Week) max_week, Merch
_Year From Wkly_Sas_Prod_Time_Pd where merch_year = rec.merch_year group by merc
h_year)                                                                         

                                                                                
   Loop                                                                         
                                                                                
      Insert /*+ Append */ Into Wkly_Inv_Move_Extract                           
      Select /*+ FULL(I) PARALLEL(I,8) */                                       
       I.Site_Id, I.Style_Id, I.Color_Id, I.Size_Id, I.Dimension_Id,            
       I.Inven_Move_Type, I.Inven_Move_Qty, I.Inven_Move_Date,                  
       I.Retail_Price, 0 Retail_Price_Final, I.Landed_Unit_Cost, I.Average_Cost,
                                                                                
                                                                                
       I.Merchandising_Year, I.Merchandising_Week, I.Section_Id                 
      From   (Inventory_Movements@Mc2p) I                                       
      Where                                                                     

                                                                                
        I.Merchandising_Year = Rec2.Merch_Year And                              
        I.Merchandising_Week Between Rec2.Min_Week And Rec2.Max_Week;           
        Commit;                                                                 
                                                                                
   end loop;                                                                    
                                                                                
                                                                                
End Loop;                                                                       
                                                                                
End If;                                                                         
                                                                                
                                                                                
--execute immediate 'Insert /*+ Append */ Into Wkly_Inv_Move_Extract            
--      Select /*+ FULL(I) PARALLEL(I,8) */                                     
--       I.Site_Id, I.Style_Id, I.Color_Id, I.Size_Id, I.Dimension_Id,          
--       I.Inven_Move_Type, I.Inven_Move_Qty, I.Inven_Move_Date,                
--       I.Retail_Price, 0 Retail_Price_Final, I.Landed_Unit_Cost, I.Average_Cos
t,                                                                              
                                                                                
--       i.Merchandising_Year, i.Merchandising_Week, I.section_id               
-- From   (Inventory_Movements@Mc2r) I ' || v_where;                            
                                                                                

                                                                                
                                                                                
Update Sas_Process_Log Set Process_Ind = 'C', Process_End_Time = Sysdate        
Where Process_Step = 20 And Process_Id = P_Process_Id;                          
commit;                                                                         
                                                                                
                                                                                
/*Submit Next Step*/                                                            
                                                                                
Dbms_Job.Submit(Jobno, 'P_Wkly_prod_master();', Sysdate, Null);                 
Dbms_Job.Submit(Jobno, 'P_Wkly_in_out_trfs();', Sysdate, Null);                 
Commit;                                                                         
                                                                                

                                                                                
END P_WKLY_INVENTORY_EXTRACT;                                                   

156 rows selected.

SQL> SPOOL OFF
