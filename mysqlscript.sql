#drop table aliMobRec;
create table aliMobRec(
    user_id varchar(25),
    item_id  varchar(25),
    behavior_type varchar(5),
    user_goehash varchar(15),
    item_category varchar(25),
    r_time varchar(20),
    user_category_pairs varchar(50),
    days varchar(10),
    hours varchar(10),
    D_H float,
    user_item_pairs varchar(50),
    item_category_pairs varchar(50)
    );

load data infile 'C:/a.csv' into table alimobrec
fields terminated by ','  
optionally enclosed by '"' 
escaped by '"' 
lines terminated by '\n'
ignore 1 lines
(user_id,item_id,behavior_type,user_goehash,item_category,r_time);

update alimobrec set user_category_pairs = concat(user_id,',',item_category) ;
update alimobrec set days = (cast(substring(r_time,6,2) as decimal) - 11) * 30 + cast(substring(r_time, 9,2) as decimal) - 17;
update alimobrec set hours = substring(r_time, 12,2);
update alimobrec set D_H = cast(days as decimal) + cast(hours as decimal)/24;

alter table alimobrec add index INDEX_uc (user_category_pairs);
alter table alimobrec add index INDEX_u (user_id);
alter table alimobrec add index INDEX_i (item_id);
alter table alimobrec add index INDEX_c (item_category);

select * from alimobrec order by user_category_pairs LIMIT 0,30;


create table aliMobRecItemForPred(
    item_id varchar(25),
    user_goehash varchar(15),
    item_category varchar(25)
);

load data infile 'C:/b.csv' into table aliMobRecItemForPred
fields terminated by ','  
optionally enclosed by '"' 
escaped by '"' 
lines terminated by '\n'
ignore 1 lines;

select item_category,count(*) from aliMobRecItemForPred group by item_category;
select count(distinct(item_category)) from aliMobRecItemForPred;

drop table userCategoryForPred;
create table userCategoryForPred(
    user_id varchar(25),
    item_category varchar(25),
    user_category_pairs varchar(50)
);

update userCategoryForPred set user_category_pairs = concat(user_id,',',item_category) ;
alter table userCategoryForPred add index INDEX_uc (user_category_pairs);
alter table userCategoryForPred add index INDEX_u (user_id);
alter table userCategoryForPred add index INDEX_c (item_category);

select * from userCategoryForPred where user_id = '100014756';
select * from userCategoryForPred where user_category_pairs = '100014756,1032';
select count(*) from userCategoryForPred;
select distinct(item_category) from usercategoryforpred 

