drop table aliMobRec;
create table aliMobRec(
    user_id varchar(25),
    item_id  varchar(25),
    behavior_type varchar(5),
    user_goehash varchar(15),
    item_category varchar(25),
    r_time varchar(20)
    );

load data infile 'C:/a.csv' into table alimobrec
fields terminated by ','  
optionally enclosed by '"' 
escaped by '"' 
lines terminated by '\n'
ignore 1 lines;

set global connect_timeout=6000;

alter table alimobrec add user_category_pairs varchar(50);
update alimobrec set user_category_pairs = concat(user_id,',',item_category) ;
alter table alimobrec add days varchar(10);
update alimobrec set days = substring(r_time, 9,2);
alter table alimobrec add hours varchar(10);
update alimobrec set hours = substring(r_time, 12,2);
alter table alimobrec add D_H float;
update alimobrec set D_H = cast(days as decimal) + cast(hours as decimal)/24;

alter table alimobrec add index INDEX_uc (user_category_pairs);
alter table alimobrec add index INDEX_u (user_id);
alter table alimobrec add index INDEX_i (item_id);
alter table alimobrec add index INDEX_c (item_category);

select * from alimobrec order by user_category_pairs LIMIT 0,30;
select * from alimobrec order by user_id LIMIT 0, 10;
select * from alimobrec where user_category_pairs = '100014756,11824';


select user_category_pairs,count(*) from alimobrec
group by user_category_pairs