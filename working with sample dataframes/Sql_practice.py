/*creating a table called emp*/
create table emp(firstName varchar(30),middleName varchar(30),lastName varchar(30),dob int, gender char(1),salary int )

/*inserting values into the table*/


-- COMMAND ----------

insert into emp values('James','','Smith',03011998,'M',3000);
insert into emp values('Michael','Rose','',10011998,'M',20000);
insert into emp values('Robert','','Williams',02012000,'M',3000);
insert into emp values('Maria','Annie','Jones',03011998,'F',11000);
insert into emp values('Jen','Mary','Brown',04101998,'F',10000);

-- COMMAND ----------

select * from emp

-- COMMAND ----------

--selecting firstName,lastName,Salary column from the table emp

select firstName,lastName,salary from emp;

-- COMMAND ----------

--adding new column Country,Department,Age to the table emp
alter table emp add column Country varchar(20);

-- COMMAND ----------

alter table emp add column Department varchar(20);

-- COMMAND ----------

alter table emp add column age int;

-- COMMAND ----------

select * from emp

-- COMMAND ----------

update emp set Country='india' where firstName='Maria'


-- COMMAND ----------

alter table emp drop column Department;

-- COMMAND ----------

select * from emp;

-- COMMAND ----------

update emp set Country='india' where firstName='James'

-- COMMAND ----------

update emp set Country='india'

-- COMMAND ----------

select * from emp;

-- COMMAND ----------

update emp set Department='Hr'


-- COMMAND ----------

update emp set age=46


-- COMMAND ----------

select * from emp;

-- COMMAND ----------

update emp set age=30 where age=46

-- COMMAND ----------

select * from emp;

-- COMMAND ----------

--insert into emp(Country,Department,age) values ('india','hr',16),('india','hr',17),('india','hr',19),('india','hr',20),('india','hr',21);

-- COMMAND ----------

--#updating the salary column value by incrementing it to 1000
update emp set salary=salary+1000

-- COMMAND ----------

select * from emp

-- COMMAND ----------

select salary+5000 as new_salary from emp;

-- COMMAND ----------

select distinct dob from emp;

-- COMMAND ----------

select * , salary+500 as new_salary from emp;

-- COMMAND ----------

--changing the datatype of dob and salary column to string
---alter table emp alter column dob varchar(10)

-- COMMAND ----------

ALTER TABLE emp
CHANGE COLUMN firstname TO new_name;

-- COMMAND ----------

select * from emp;

-- COMMAND ----------

create table student2(student_id int,student_name varchar(20),student_age int,student_avg decimal(10,2))

-- COMMAND ----------

insert into student2 values(1,'sree',23,45.5),(2,'kanth',35,78.9);

-- COMMAND ----------

select * from student2;


-- COMMAND ----------

insert into student2(student_id,student_name) values (3,'sun');

-- COMMAND ----------

insert into student2(student_id,student_name,student_age,student_avg) values (3,'sun',null,null);

-- COMMAND ----------

select * from student2;

-- COMMAND ----------

Alter table student2
rename students;

-- COMMAND ----------

rename student2 to studentss

-- COMMAND ----------

rename table student2 to students;

-- COMMAND ----------

ALTER TABLE student2
CHANGE COLUMN student_id TO new_name;