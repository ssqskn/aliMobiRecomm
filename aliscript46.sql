###########################user-features####################################################
create table aliUser46(
    user_id varchar(25),
    V1  float,  V2  float, V3  float, V4  float, V5  float, V6  float,
    V7  float,  V8  float, V9  float, V10  float,V11  float,V12  float,
	V13  float, V14  float,V15  float,V16  float,V17 float,V18 float,
    V19 float, V20 float, V21 float, V22 float, V23 float, V24 float,
    V25 float, V26 float, V27 float, V28 float, V29 float, V30 float
    );

insert into aliUser46(user_id) select distinct(user_id) from aliMobRec;
alter table aliUser46 add index INDEX_u(user_id);

update aliUser46 b set b.V1 = (select c.cnt from    
(select user_id,count(user_id) cnt from alimobrec where D_H < 47 group by user_id) c where b.user_id = c.user_id);
update aliUser46 b set b.V2 = (select c.cnt from  
(select user_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 group by user_id) c where b.user_id = c.user_id);
update aliUser46 b set b.V3 = (select c.cnt from  
(select user_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 group by user_id) c where b.user_id = c.user_id);
update aliUser46 b set b.V4 = (select c.cnt from   
(select user_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 group by user_id) c where b.user_id = c.user_id);
update aliUser46 b set b.V5 = (select c.cnt from 
(select user_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 group by user_id) c where b.user_id = c.user_id);
update aliUser46 b set b.V6 = (select c.cnt from  
(select user_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 and D_H >= 46 group by user_id) c where b.user_id = c.user_id);
update aliUser46 b set b.V7 = (select c.cnt from   
(select user_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 and D_H >= 46 group by user_id) c where b.user_id = c.user_id);
update aliUser46 b set b.V8 = (select c.cnt from   
(select user_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 and D_H >= 46 group by user_id) c where b.user_id = c.user_id);
update aliUser46 b set b.V9 = (select c.cnt from   
(select user_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 and D_H >= 46 group by user_id) c where b.user_id = c.user_id);
update aliUser46 b set b.V10 = (select c.cnt from  
(select user_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 and D_H >= 44 group by user_id) c where b.user_id = c.user_id);
update aliUser46 b set b.V11 = (select c.cnt from   
(select user_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 and D_H >= 44 group by user_id) c where b.user_id = c.user_id);
update aliUser46 b set b.V12 = (select c.cnt from  
(select user_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 and D_H >= 44 group by user_id) c where b.user_id = c.user_id);
update aliUser46 b set b.V13 = (select c.cnt from  
(select user_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 and D_H >= 44 group by user_id) c where b.user_id = c.user_id);




###########################item-features####################################################
create table aliItem46(
    item_id varchar(25),
    V1  float,  V2  float, V3  float, V4  float, V5  float, V6  float,
    V7  float,  V8  float, V9  float, V10  float,V11  float,V12  float,
	V13  float, V14  float,V15  float,V16  float,V17 float,V18 float,
    V19 float, V20 float, V21 float, V22 float, V23 float, V24 float,
    V25 float, V26 float, V27 float, V28 float, V29 float, V30 float
    );

insert into aliItem46(item_id) select distinct(item_id) from aliMobRec;
alter table aliItem46 add index INDEX_i(item_id);

update aliItem46 b set b.V1 = (select c.cnt from    
(select item_id,count(item_id) cnt from alimobrec where D_H < 47 group by item_id) c where b.item_id = c.item_id);
update aliItem46 b set b.V2 = (select c.cnt from  
(select item_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 group by item_id) c where b.item_id = c.item_id);
update aliItem46 b set b.V3 = (select c.cnt from    
(select item_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 group by item_id) c where b.item_id = c.item_id);
update aliItem46 b set b.V4 = (select c.cnt from    
(select item_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 group by item_id) c where b.item_id = c.item_id);
update aliItem46 b set b.V5 = (select c.cnt from    
(select item_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 group by item_id) c where b.item_id = c.item_id);
update aliItem46 b set b.V6 = (select c.cnt from   
(select item_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 and D_H >= 46 group by item_id) c where b.item_id = c.item_id);
update aliItem46 b set b.V7 = (select c.cnt from    
(select item_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 and D_H >= 46 group by item_id) c where b.item_id = c.item_id);
update aliItem46 b set b.V8 = (select c.cnt from  
(select item_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 and D_H >= 46 group by item_id) c where b.item_id = c.item_id);
update aliItem46 b set b.V9 = (select c.cnt from   
(select item_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 and D_H >= 46 group by item_id) c where b.item_id = c.item_id);
update aliItem46 b set b.V10 = (select c.cnt from    
(select item_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 and D_H >= 44 group by item_id) c where b.item_id = c.item_id);
update aliItem46 b set b.V11 = (select c.cnt from     
(select item_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 and D_H >= 44 group by item_id) c where b.item_id = c.item_id);
update aliItem46 b set b.V12 = (select c.cnt from    
(select item_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 and D_H >= 44 group by item_id) c where b.item_id = c.item_id);
update aliItem46 b set b.V13 = (select c.cnt from  
(select item_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 and D_H >= 44 group by item_id) c where b.item_id = c.item_id);


#############################user-item pairs###################################
create table aliUserItem46(
    user_item_pairs varchar(25),user_id varchar(25),item_id varchar(25),
    item_category varchar(25), user_category_pairs varchar(50),
    V1  float,  V2  float, V3  float, V4  float, V5  float, V6  float,
    V7  float,  V8  float, V9  float, V10  float,V11  float,V12  float,
	V13  float, V14  float,V15  float,V16  float,V17 float,V18 float,
    V19 float, V20 float, V21 float, V22 float, V23 float, V24 float,
    V25 float, V26 float, V27 float, V28 float, V29 float, V30 float
    );

insert into aliUserItem46(user_item_pairs) select distinct(user_item_pairs) from aliMobRec;
alter table aliUserItem46 add index INDEX_ui(user_item_pairs);

update aliUserItem46 b set b.V1 = (select c.cnt from    
(select user_item_pairs,count(user_item_pairs) cnt from alimobrec where D_H < 47 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.V2 = (select c.cnt from      
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.V3 = (select c.cnt from     
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.V4 = (select c.cnt from  
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.V5 = (select c.cnt from   
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.V6 = (select c.cnt from   
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 and D_H >= 46 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.V7 = (select c.cnt from   
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 and D_H >= 46 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.V8 = (select c.cnt from  
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 and D_H >= 46 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.V9 = (select c.cnt from    
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 and D_H >= 46 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.V10 = (select c.cnt from   
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 and D_H >= 44 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.V11 = (select c.cnt from     
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 and D_H >= 44 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.V12 = (select c.cnt from  
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 and D_H >= 44 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.V13 = (select c.cnt from 
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 and D_H >= 44 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.V14 = (select c.cnt from 
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 and D_H >= 41 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.V15 = (select c.cnt from  
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 and D_H >= 41 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.V16 = (select c.cnt from 
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 and D_H >= 41 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.V17 = (select c.cnt from 
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 and D_H >= 41 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);

update aliUserItem46 b set b.V18 = (select c.mx from   
(select user_item_pairs,max(behavior_type) mx from alimobrec where behavior_type = '4' and D_H >= 47 and D_H < 48 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);

###########################category-features####################################################
create table alicategory46(
    item_category varchar(25),
    V1  float,  V2  float, V3  float, V4  float, V5  float, V6  float,
    V7  float,  V8  float, V9  float, V10  float,V11  float,V12  float,
	V13  float, V14  float,V15  float,V16  float,V17 float,V18 float,
    V19 float, V20 float, V21 float, V22 float, V23 float, V24 float,
    V25 float, V26 float, V27 float, V28 float, V29 float, V30 float
    );

insert into alicategory46(item_category) select distinct(item_category) from aliMobRec;
alter table alicategory46 add index INDEX_c(item_category);

update alicategory46 b set b.V1 = (select c.cnt from   
(select item_category,count(item_category) cnt from alimobrec where D_H < 47 group by item_category) c where b.item_category = c.item_category);
update alicategory46 b set b.V2 = (select c.cnt from     
(select item_category,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 group by item_category) c where b.item_category = c.item_category);
update alicategory46 b set b.V3 = (select c.cnt from   
(select item_category,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 group by item_category) c where b.item_category = c.item_category);
update alicategory46 b set b.V4 = (select c.cnt from     
(select item_category,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 group by item_category) c where b.item_category = c.item_category);
update alicategory46 b set b.V5 = (select c.cnt from     
(select item_category,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 group by item_category) c where b.item_category = c.item_category);
update alicategory46 b set b.V6 = (select c.cnt from    
(select item_category,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 and D_H >= 46 group by item_category) c where b.item_category = c.item_category);
update alicategory46 b set b.V7 = (select c.cnt from   
(select item_category,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 and D_H >= 46 group by item_category) c where b.item_category = c.item_category);
update alicategory46 b set b.V8 = (select c.cnt from   
(select item_category,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 and D_H >= 46 group by item_category) c where b.item_category = c.item_category);
update alicategory46 b set b.V9 = (select c.cnt from   
(select item_category,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 and D_H >= 46 group by item_category) c where b.item_category = c.item_category);
update alicategory46 b set b.V10 = (select c.cnt from     
(select item_category,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 and D_H >= 44 group by item_category) c where b.item_category = c.item_category);
update alicategory46 b set b.V11 = (select c.cnt from     
(select item_category,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 and D_H >= 44 group by item_category) c where b.item_category = c.item_category);
update alicategory46 b set b.V12 = (select c.cnt from    
(select item_category,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 and D_H >= 44 group by item_category) c where b.item_category = c.item_category);
update alicategory46 b set b.V13 = (select c.cnt from   
(select item_category,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 and D_H >= 44 group by item_category) c where b.item_category = c.item_category);


#############################user-category pairs###################################
create table aliUserCategory46(
    user_category_pairs varchar(25),
    V1  float,  V2  float, V3  float, V4  float, V5  float, V6  float,
    V7  float,  V8  float, V9  float, V10  float,V11  float,V12  float,
	V13  float, V14  float,V15  float,V16  float,V17 float,V18 float,
    V19 float, V20 float, V21 float, V22 float, V23 float, V24 float,
    V25 float, V26 float, V27 float, V28 float, V29 float, V30 float
    );

insert into aliUserCategory46(user_category_pairs) select distinct(user_category_pairs) from aliMobRec;
alter table aliUserCategory46 add index INDEX_uc(user_category_pairs);

update aliUserCategory46 b set b.V1 = (select c.cnt from   
(select user_category_pairs,count(user_category_pairs) cnt from alimobrec where D_H < 47 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUserCategory46 b set b.V2 = (select c.cnt from  
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUserCategory46 b set b.V3 = (select c.cnt from   
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUserCategory46 b set b.V4 = (select c.cnt from  
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUserCategory46 b set b.V5 = (select c.cnt from 
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUserCategory46 b set b.V6 = (select c.cnt from  
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 and D_H >= 46 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUserCategory46 b set b.V7 = (select c.cnt from    
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 and D_H >= 46 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUserCategory46 b set b.V8 = (select c.cnt from   
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 and D_H >= 46 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUserCategory46 b set b.V9 = (select c.cnt from    
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 and D_H >= 46 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUserCategory46 b set b.V10 = (select c.cnt from    
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 and D_H >= 44 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUserCategory46 b set b.V11 = (select c.cnt from    
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 and D_H >= 44 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUserCategory46 b set b.V12 = (select c.cnt from   
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 and D_H >= 44 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUserCategory46 b set b.V13 = (select c.cnt from    
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 and D_H >= 44 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUserCategory46 b set b.V14 = (select c.cnt from   
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 47 and D_H >= 41 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUserCategory46 b set b.V15 = (select c.cnt from   
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 47 and D_H >= 41 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUserCategory46 b set b.V16 = (select c.cnt from    
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 47 and D_H >= 41 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUserCategory46 b set b.V17 = (select c.cnt from     
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 47 and D_H >= 41 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);

#################combination################
update aliUserItem46 b set b.user_id = (select user_id from
(select user_id,user_item_pairs from aliMobrec group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.item_id  = (select item_id from
(select item_id,user_item_pairs from alimobrec group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.item_category  = (select item_category from
(select item_category,user_item_pairs from alimobrec group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem46 b set b.user_category_pairs  = (select user_category_pairs from
(select user_category_pairs,user_item_pairs from alimobrec group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);

alter table aliUserItem46 add index INDEX_uc (user_category_pairs);
alter table aliUserItem46 add index INDEX_u (user_id);
alter table aliUserItem46 add index INDEX_i (item_id);
alter table aliUserItem46 add index INDEX_c (item_category);

##################方法1###################
create table traintmp1 as (select 
b.user_item_pairs,b.user_id,b.item_id,b.item_category,b.user_category_pairs,b.V1 as V1b,b.V2 as v2b,b.V3 as v3b,b.V4 as v4b,b.V5 as v5b,b.V6 as v6b,b.V7 as v7b,b.V8 as v8b,
b.V9 as v9b,b.v10 as v10b,b.v11 as v11b,b.v12 as v12b,b.v13 as v13b,b.v14 as v14b,b.v15 as v15b,b.v16 as v16b,b.v17 as v17b,b.v18 as target,
c.V1 as v1c,c.v2 as v2c,c.v3 as v3c,c.v4 as v4c,c.v5 as v5c,c.v6 as v6c,c.v7 as v7c,c.v8 as v8c,c.v9 as v9c,c.v10 as v10c,c.v11 as v11c,c.v12 as v12c,c.v13 as v13c
from aliUserItem46 b,aliUser46 c
where  b.user_id = c.user_id);

alter table traintmp1 add index INDEX_c(item_category);

create table traintmp2 as (select 
a.*,d.V1 as v1d,d.v2 as v2d,d.v3 as v3d,d.v4 as v4d,d.v5 as v5d,d.v6 as v6d,d.v7 as v7d,d.v8 as v8d,d.v9 as v9d,d.v10 as v10d,d.v11 as v11d,d.v12 as v12d,d.v13 as v13d
from traintmp1 a,aliCategory46 d
where a.item_category = d.item_category);

alter table traintmp2 add index INDEX_uc(user_category_pairs);

create table traintmp3 as (select
a.*,e.V1 as v1e,e.v2 as v2e,e.v3 as v3e,e.v4 as v4e,e.v5 as v5e,e.v6 as v6e,e.v7 as v7e,e.v8 as v8e,e.v9 as v9e,e.v10 as v10e,e.v11 as v11e,e.v12 as v12e,e.v13 as v13e,e.v14 as v14e,e.v15 as v15e,e.v16 as v16e,e.v17 as v17e
from traintmp2 a,aliUserCategory46 e
where a.user_category_pairs = e.user_category_pairs);

alter table traintmp3 add index INDEX_i(item_id);

create table train46 as (select
a.*,f.V1 as v1f,f.v2 as v2f,f.v3 as v3f,f.v4 as v4f,f.v5 as v5f,f.v6 as v6f,f.v7 as v7f,f.v8 as v8f,f.v9 as v9f,f.v10 as v10f,f.v11 as v11f,f.v12 as v12f,f.v13 as v13f
from traintmp3 a,aliItem46 f
where a.item_id = f.item_id);

drop table traintmp1;
drop table traintmp2;
drop table traintmp3;

alter table train46 add index INDEX_tgt(target);
