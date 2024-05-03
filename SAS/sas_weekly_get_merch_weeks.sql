
set head off
set linesize 1000
set colsep ,
set pagesize 0
set tab off
set wrap off
Select 'rec',Mc.Merchandising_Year, Mc.Merchandising_Week, Mc.Merchandising_Period
From (Select * From Merchandising_Calendars Where Business_Unit_Id = 30 And Week_Ending_Date <= Trunc(Sysdate) 
Order By Merchandising_Year Desc, Merchandising_Week Desc) Mc
          Where Rownum <= 5;
exit
