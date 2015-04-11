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
update alimobrec set user_item_pairs = concat(user_id,',',item_id) ;
update alimobrec set days = substring(r_time, 9,2);
update alimobrec set hours = substring(r_time, 12,2);
update alimobrec set D_H = cast((substring(r_time, 6,2) - 11) as decimal) * 30 + cast(days as decimal) + cast(hours as decimal)/24;

alter table alimobrec add index INDEX_uc (user_category_pairs);
alter table alimobrec add index INDEX_u (user_id);
alter table alimobrec add index INDEX_i (item_id);
alter table alimobrec add index INDEX_c (item_category);
alter table alimobrec add index INDEX_ui (user_item_pairs);

###########################user-features####################################################
create table aliUser(
    user_id varchar(25),
    V1  float,  V2  float, V3  float, V4  float, V5  float, V6  float,
    V7  float,  V8  float, V9  float, V10  float,V11  float,V12  float,
	V13  float, V14  float,V15  float,V16  float,V17 float,V18 float,
    V19 float, V20 float, V21 float, V22 float, V23 float, V24 float,
    V25 float, V26 float, V27 float, V28 float, V29 float, V30 float
    );

insert into aliUser(user_id) select distinct(user_id) from aliMobRec;
update aliUser b set b.V1 = (select c.cnt from      ##V1 - 每个用户记录数,除去最后一天
(select user_id,count(user_id) cnt from alimobrec where D_H < 48 group by user_id) c where b.user_id = c.user_id);
update aliUser b set b.V2 = (select c.cnt from      ##V2 - 每个用户浏览数,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 group by user_id) c where b.user_id = c.user_id);
update aliUser b set b.V3 = (select c.cnt from      ##V3 - 每个用户收藏,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 group by user_id) c where b.user_id = c.user_id);
update aliUser b set b.V4 = (select c.cnt from      ##V4 - 每个用户购物车,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 group by user_id) c where b.user_id = c.user_id);
update aliUser b set b.V5 = (select c.cnt from      ##V5 - 每个用户购买,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 group by user_id) c where b.user_id = c.user_id);
update aliUser b set b.V6 = (select c.cnt from      ##V6 - 每个用户前一天浏览数
(select user_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 and D_H >= 47 group by user_id) c where b.user_id = c.user_id);
update aliUser b set b.V7 = (select c.cnt from      ##V7 - 每个用户前一天收藏,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 and D_H >= 47 group by user_id) c where b.user_id = c.user_id);
update aliUser b set b.V8 = (select c.cnt from      ##V8 - 每个用户前一天购物车,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 and D_H >= 47 group by user_id) c where b.user_id = c.user_id);
update aliUser b set b.V9 = (select c.cnt from      ##V9 - 每个用户前一天购买,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 and D_H >= 47 group by user_id) c where b.user_id = c.user_id);
update aliUser b set b.V10 = (select c.cnt from      ##V10 - 每个用户前3天浏览数
(select user_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 and D_H >= 45 group by user_id) c where b.user_id = c.user_id);
update aliUser b set b.V11 = (select c.cnt from      ##V11 - 每个用户前3天收藏,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 and D_H >= 45 group by user_id) c where b.user_id = c.user_id);
update aliUser b set b.V12 = (select c.cnt from      ##V12 - 每个用户前3天购物车,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 and D_H >= 45 group by user_id) c where b.user_id = c.user_id);
update aliUser b set b.V13 = (select c.cnt from      ##V13 - 每个用户前3天购买,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 and D_H >= 45 group by user_id) c where b.user_id = c.user_id);




###########################item-features####################################################
create table aliItem(
    item_id varchar(25),
    V1  float,  V2  float, V3  float, V4  float, V5  float, V6  float,
    V7  float,  V8  float, V9  float, V10  float,V11  float,V12  float,
	V13  float, V14  float,V15  float,V16  float,V17 float,V18 float,
    V19 float, V20 float, V21 float, V22 float, V23 float, V24 float,
    V25 float, V26 float, V27 float, V28 float, V29 float, V30 float
    );

insert into aliItem(item_id) select distinct(item_id) from aliMobRec;
update aliItem b set b.V1 = (select c.cnt from      ##V1 - 每个商品记录数,除去最后一天
(select item_id,count(item_id) cnt from alimobrec where D_H < 48 group by item_id) c where b.item_id = c.item_id);
update aliItem b set b.V2 = (select c.cnt from      ##V2 - 每个商品浏览数,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 group by item_id) c where b.item_id = c.item_id);
update aliItem b set b.V3 = (select c.cnt from      ##V3 - 每个商品收藏,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 group by item_id) c where b.item_id = c.item_id);
update aliItem b set b.V4 = (select c.cnt from      ##V4 - 每个商品购物车,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 group by item_id) c where b.item_id = c.item_id);
update aliItem b set b.V5 = (select c.cnt from      ##V5 - 每个商品购买,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 group by item_id) c where b.item_id = c.item_id);
update aliItem b set b.V6 = (select c.cnt from      ##V6 - 每个商品前一天浏览数
(select item_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 and D_H >= 47 group by item_id) c where b.item_id = c.item_id);
update aliItem b set b.V7 = (select c.cnt from      ##V7 - 每个商品前一天收藏,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 and D_H >= 47 group by item_id) c where b.item_id = c.item_id);
update aliItem b set b.V8 = (select c.cnt from      ##V8 - 每个商品前一天购物车,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 and D_H >= 47 group by item_id) c where b.item_id = c.item_id);
update aliItem b set b.V9 = (select c.cnt from      ##V9 - 每个商品前一天购买,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 and D_H >= 47 group by item_id) c where b.item_id = c.item_id);
update aliItem b set b.V10 = (select c.cnt from      ##V10 - 每个商品前3天浏览数
(select item_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 and D_H >= 45 group by item_id) c where b.item_id = c.item_id);
update aliItem b set b.V11 = (select c.cnt from      ##V11 - 每个商品前3天收藏,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 and D_H >= 45 group by item_id) c where b.item_id = c.item_id);
update aliItem b set b.V12 = (select c.cnt from      ##V12 - 每个商品前3天购物车,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 and D_H >= 45 group by item_id) c where b.item_id = c.item_id);
update aliItem b set b.V13 = (select c.cnt from      ##V13 - 每个商品前3天购买,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 and D_H >= 45 group by item_id) c where b.item_id = c.item_id);


#############################user-item pairs###################################
create table aliUserItem(
    user_item_pairs varchar(25),user_id varchar(25),item_id varchar(25),
    item_category varchar(25), user_category_pairs varchar(50),
    V1  float,  V2  float, V3  float, V4  float, V5  float, V6  float,
    V7  float,  V8  float, V9  float, V10  float,V11  float,V12  float,
	V13  float, V14  float,V15  float,V16  float,V17 float,V18 float,
    V19 float, V20 float, V21 float, V22 float, V23 float, V24 float,
    V25 float, V26 float, V27 float, V28 float, V29 float, V30 float
    );

insert into aliUserItem(user_item_pairs) select distinct(user_item_pairs) from aliMobRec;

update aliUserItem b set b.V1 = (select c.cnt from      ##V1 - 每个用户-商品对记录数,除去最后一天
(select user_item_pairs,count(user_item_pairs) cnt from alimobrec where D_H < 48 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.V2 = (select c.cnt from      ##V2 - 每个用户-商品对浏览数,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.V3 = (select c.cnt from      ##V3 - 每个用户-商品对收藏,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.V4 = (select c.cnt from      ##V4 - 每个用户-商品对购物车,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.V5 = (select c.cnt from      ##V5 - 每个用户-商品对购买,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.V6 = (select c.cnt from      ##V6 - 每个用户-商品对前一天浏览数
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 and D_H >= 47 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.V7 = (select c.cnt from      ##V7 - 每个用户-商品对前一天收藏,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 and D_H >= 47 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.V8 = (select c.cnt from      ##V8 - 每个用户-商品对前一天购物车,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 and D_H >= 47 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.V9 = (select c.cnt from      ##V9 - 每个用户-商品对前一天购买,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 and D_H >= 47 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.V10 = (select c.cnt from      ##V10 - 每个用户-商品对前3天浏览数
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 and D_H >= 45 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.V11 = (select c.cnt from      ##V11 - 每个用户-商品对前3天收藏,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 and D_H >= 45 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.V12 = (select c.cnt from      ##V12 - 每个用户-商品对前3天购物车,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 and D_H >= 45 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.V13 = (select c.cnt from      ##V13 - 每个用户-商品对前3天购买,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 and D_H >= 45 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.V14 = (select c.cnt from      ##V14 - 每个用户-商品对前7天浏览数
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 and D_H >= 41 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.V15 = (select c.cnt from      ##V15 - 每个用户-商品对前7天收藏,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 and D_H >= 41 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.V16 = (select c.cnt from      ##V16 - 每个用户-商品对前7天购物车,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 and D_H >= 41 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.V17 = (select c.cnt from      ##V17 - 每个用户-商品对前7天购买,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 and D_H >= 41 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);

update aliUserItem b set b.V18 = (select c.mx from      ##V18 - 12月18日是否购买，购买-4 未购买-空
(select user_item_pairs,max(behavior_type) mx from alimobrec where behavior_type = '4' and D_H >= 48 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
###########################category-features####################################################
create table alicategory(
    item_category varchar(25),
    V1  float,  V2  float, V3  float, V4  float, V5  float, V6  float,
    V7  float,  V8  float, V9  float, V10  float,V11  float,V12  float,
	V13  float, V14  float,V15  float,V16  float,V17 float,V18 float,
    V19 float, V20 float, V21 float, V22 float, V23 float, V24 float,
    V25 float, V26 float, V27 float, V28 float, V29 float, V30 float
    );

insert into alicategory(item_category) select distinct(item_category) from aliMobRec;
update alicategory b set b.V1 = (select c.cnt from      ##V1 - 每个类别记录数,除去最后一天
(select item_category,count(item_category) cnt from alimobrec where D_H < 48 group by item_category) c where b.item_category = c.item_category);
update alicategory b set b.V2 = (select c.cnt from      ##V2 - 每个类别浏览数,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 group by item_category) c where b.item_category = c.item_category);
update alicategory b set b.V3 = (select c.cnt from      ##V3 - 每个类别收藏,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 group by item_category) c where b.item_category = c.item_category);
update alicategory b set b.V4 = (select c.cnt from      ##V4 - 每个类别购物车,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 group by item_category) c where b.item_category = c.item_category);
update alicategory b set b.V5 = (select c.cnt from      ##V5 - 每个类别购买,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 group by item_category) c where b.item_category = c.item_category);
update alicategory b set b.V6 = (select c.cnt from      ##V6 - 每个类别前一天浏览数
(select item_category,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 and D_H >= 47 group by item_category) c where b.item_category = c.item_category);
update alicategory b set b.V7 = (select c.cnt from      ##V7 - 每个类别前一天收藏,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 and D_H >= 47 group by item_category) c where b.item_category = c.item_category);
update alicategory b set b.V8 = (select c.cnt from      ##V8 - 每个类别前一天购物车,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 and D_H >= 47 group by item_category) c where b.item_category = c.item_category);
update alicategory b set b.V9 = (select c.cnt from      ##V9 - 每个类别前一天购买,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 and D_H >= 47 group by item_category) c where b.item_category = c.item_category);
update alicategory b set b.V10 = (select c.cnt from      ##V10 - 每个类别前3天浏览数
(select item_category,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 and D_H >= 45 group by item_category) c where b.item_category = c.item_category);
update alicategory b set b.V11 = (select c.cnt from      ##V11 - 每个类别前3天收藏,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 and D_H >= 45 group by item_category) c where b.item_category = c.item_category);
update alicategory b set b.V12 = (select c.cnt from      ##V12 - 每个类别前3天购物车,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 and D_H >= 45 group by item_category) c where b.item_category = c.item_category);
update alicategory b set b.V13 = (select c.cnt from      ##V13 - 每个类别前3天购买,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 and D_H >= 45 group by item_category) c where b.item_category = c.item_category);


#############################user-category pairs###################################
create table aliUsercategory(
    user_category_pairs varchar(25),
    V1  float,  V2  float, V3  float, V4  float, V5  float, V6  float,
    V7  float,  V8  float, V9  float, V10  float,V11  float,V12  float,
	V13  float, V14  float,V15  float,V16  float,V17 float,V18 float,
    V19 float, V20 float, V21 float, V22 float, V23 float, V24 float,
    V25 float, V26 float, V27 float, V28 float, V29 float, V30 float
    );

insert into aliUsercategory(user_category_pairs) select distinct(user_category_pairs) from aliMobRec;
update aliUsercategory b set b.V1 = (select c.cnt from      ##V1 - 每个用户-类别对记录数,除去最后一天
(select user_category_pairs,count(user_category_pairs) cnt from alimobrec where D_H < 48 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUsercategory b set b.V2 = (select c.cnt from      ##V2 - 每个用户-类别对浏览数,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUsercategory b set b.V3 = (select c.cnt from      ##V3 - 每个用户-类别对收藏,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUsercategory b set b.V4 = (select c.cnt from      ##V4 - 每个用户-类别对购物车,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUsercategory b set b.V5 = (select c.cnt from      ##V5 - 每个用户-类别对购买,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUsercategory b set b.V6 = (select c.cnt from      ##V6 - 每个用户-类别对前一天浏览数
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 and D_H >= 47 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUsercategory b set b.V7 = (select c.cnt from      ##V7 - 每个用户-类别对前一天收藏,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 and D_H >= 47 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUsercategory b set b.V8 = (select c.cnt from      ##V8 - 每个用户-类别对前一天购物车,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 and D_H >= 47 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUsercategory b set b.V9 = (select c.cnt from      ##V9 - 每个用户-类别对前一天购买,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 and D_H >= 47 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUsercategory b set b.V10 = (select c.cnt from      ##V10 - 每个用户-类别对前3天浏览数
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 and D_H >= 45 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUsercategory b set b.V11 = (select c.cnt from      ##V11 - 每个用户-类别对前3天收藏,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 and D_H >= 45 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUsercategory b set b.V12 = (select c.cnt from      ##V12 - 每个用户-类别对前3天购物车,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 and D_H >= 45 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUsercategory b set b.V13 = (select c.cnt from      ##V13 - 每个用户-类别对前3天购买,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 and D_H >= 45 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUsercategory b set b.V14 = (select c.cnt from      ##V14 - 每个用户-类别对前7天浏览数
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H < 48 and D_H >= 41 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUsercategory b set b.V15 = (select c.cnt from      ##V15 - 每个用户-类别对前7天收藏,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H < 48 and D_H >= 41 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUsercategory b set b.V16 = (select c.cnt from      ##V16 - 每个用户-类别对前7天购物车,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H < 48 and D_H >= 41 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliUsercategory b set b.V17 = (select c.cnt from      ##V17 - 每个用户-类别对前7天购买,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H < 48 and D_H >= 41 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);

#################combination################
update aliUserItem b set b.user_id = (select user_id from
(select user_id,user_item_pairs from aliMobrec group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.item_id  = (select item_id from
(select item_id,user_item_pairs from alimobrec group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.item_category  = (select item_category from
(select item_category,user_item_pairs from alimobrec group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItem b set b.user_category_pairs  = (select user_category_pairs from
(select user_category_pairs,user_item_pairs from alimobrec group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);

alter table aliUser add index INDEX_u(user_id);
alter table aliItem add index INDEX_i(item_id);
alter table aliCategory add index INDEX_c(item_category);
alter table aliUserCategory add index INDEX_uc(user_category_pairs);
alter table aliUserItem add index INDEX_ui(user_item_pairs);
alter table aliUserItem add index INDEX_uc (user_category_pairs);
alter table aliUserItem add index INDEX_u (user_id);
alter table aliUserItem add index INDEX_i (item_id);
alter table aliUserItem add index INDEX_c (item_category);

##################方法1###################
create table traintmp1 as (select 
b.user_item_pairs,b.user_id,b.item_id,b.item_category,b.user_category_pairs,b.V1 as V1b,b.V2 as v2b,b.V3 as v3b,b.V4 as v4b,b.V5 as v5b,b.V6 as v6b,b.V7 as v7b,b.V8 as v8b,
b.V9 as v9b,b.v10 as v10b,b.v11 as v11b,b.v12 as v12b,b.v13 as v13b,b.v14 as v14b,b.v15 as v15b,b.v16 as v16b,b.v17 as v17b,b.v18 as target,
c.V1 as v1c,c.v2 as v2c,c.v3 as v3c,c.v4 as v4c,c.v5 as v5c,c.v6 as v6c,c.v7 as v7c,c.v8 as v8c,c.v9 as v9c,c.v10 as v10c,c.v11 as v11c,c.v12 as v12c,c.v13 as v13c
from aliUserItem b,aliUser c
where  b.user_id = c.user_id);

alter table traintmp1 add index INDEX_c(item_category);

create table traintmp2 as (select 
a.*,d.V1 as v1d,d.v2 as v2d,d.v3 as v3d,d.v4 as v4d,d.v5 as v5d,d.v6 as v6d,d.v7 as v7d,d.v8 as v8d,d.v9 as v9d,d.v10 as v10d,d.v11 as v11d,d.v12 as v12d,d.v13 as v13d
from traintmp1 a,aliCategory d
where a.item_category = d.item_category);

alter table traintmp2 add index INDEX_uc(user_category_pairs);

create table traintmp3 as (select
a.*,e.V1 as v1e,e.v2 as v2e,e.v3 as v3e,e.v4 as v4e,e.v5 as v5e,e.v6 as v6e,e.v7 as v7e,e.v8 as v8e,e.v9 as v9e,e.v10 as v10e,e.v11 as v11e,e.v12 as v12e,e.v13 as v13e,e.v14 as v14e,e.v15 as v15e,e.v16 as v16e,e.v17 as v17e
from traintmp2 a,aliUserCategory e
where a.user_category_pairs = e.user_category_pairs);

alter table traintmp3 add index INDEX_i(item_id);

create table train as (select
a.*,f.V1 as v1f,f.v2 as v2f,f.v3 as v3f,f.v4 as v4f,f.v5 as v5f,f.v6 as v6f,f.v7 as v7f,f.v8 as v8f,f.v9 as v9f,f.v10 as v10f,f.v11 as v11f,f.v12 as v12f,f.v13 as v13f
from traintmp3 a,aliItem f
where a.item_id = f.item_id);

drop table traintmp1;
drop table traintmp2;
drop table traintmp3;
#########################################################
####################TEST SET#############################
#########################################################
###########################user-features  Test####################################################
create table aliUserTest(
    user_id varchar(25),
    V1  float,  V2  float, V3  float, V4  float, V5  float, V6  float,
    V7  float,  V8  float, V9  float, V10  float,V11  float,V12  float,
  	V13  float, V14  float,V15  float,V16  float,V17 float,V18 float,
    V19 float, V20 float, V21 float, V22 float, V23 float, V24 float,
    V25 float, V26 float, V27 float, V28 float, V29 float, V30 float
    );

insert into aliUserTest(user_id) select distinct(user_id) from alimobrec;
update aliUserTest b set b.V1 = (select c.cnt from      ##V1 - 每个用户记录数,除去最后一天
(select user_id,count(user_id) cnt from alimobrec where D_H > 19 group by user_id) c where b.user_id = c.user_id);
update aliUserTest b set b.V2 = (select c.cnt from      ##V2 - 每个用户浏览数,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H > 19  group by user_id) c where b.user_id = c.user_id);
update aliUserTest b set b.V3 = (select c.cnt from      ##V3 - 每个用户收藏,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 group by user_id) c where b.user_id = c.user_id);
update aliUserTest b set b.V4 = (select c.cnt from      ##V4 - 每个用户购物车,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 group by user_id) c where b.user_id = c.user_id);
update aliUserTest b set b.V5 = (select c.cnt from      ##V5 - 每个用户购买,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 group by user_id) c where b.user_id = c.user_id);
update aliUserTest b set b.V6 = (select c.cnt from      ##V6 - 每个用户前一天浏览数
(select user_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H  >19 and D_H >= 48 group by user_id) c where b.user_id = c.user_id);
update aliUserTest b set b.V7 = (select c.cnt from      ##V7 - 每个用户前一天收藏,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 and D_H >= 48 group by user_id) c where b.user_id = c.user_id);
update aliUserTest b set b.V8 = (select c.cnt from      ##V8 - 每个用户前一天购物车,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 and D_H >= 48 group by user_id) c where b.user_id = c.user_id);
update aliUserTest b set b.V9 = (select c.cnt from      ##V9 - 每个用户前一天购买,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 and D_H >= 48 group by user_id) c where b.user_id = c.user_id);
update aliUserTest b set b.V10 = (select c.cnt from      ##V10 - 每个用户前3天浏览数
(select user_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H  >19 and D_H >= 46 group by user_id) c where b.user_id = c.user_id);
update aliUserTest b set b.V11 = (select c.cnt from      ##V11 - 每个用户前3天收藏,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 and D_H >= 46 group by user_id) c where b.user_id = c.user_id);
update aliUserTest b set b.V12 = (select c.cnt from      ##V12 - 每个用户前3天购物车,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 and D_H >= 46 group by user_id) c where b.user_id = c.user_id);
update aliUserTest b set b.V13 = (select c.cnt from      ##V13 - 每个用户前3天购买,除去最后一天
(select user_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 and D_H >= 46 group by user_id) c where b.user_id = c.user_id);




###########################item-features  Test####################################################
create table aliItemTest(
    item_id varchar(25),
    V1  float,  V2  float, V3  float, V4  float, V5  float, V6  float,
    V7  float,  V8  float, V9  float, V10  float,V11  float,V12  float,
	V13  float, V14  float,V15  float,V16  float,V17 float,V18 float,
    V19 float, V20 float, V21 float, V22 float, V23 float, V24 float,
    V25 float, V26 float, V27 float, V28 float, V29 float, V30 float
    );

insert into aliItemTest(item_id) select distinct(item_id) from alimobrec;
update aliItemTest b set b.V1 = (select c.cnt from      ##V1 - 每个商品记录数,除去最后一天
(select item_id,count(item_id) cnt from alimobrec where D_H  >19 group by item_id) c where b.item_id = c.item_id);
update aliItemTest b set b.V2 = (select c.cnt from      ##V2 - 每个商品浏览数,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H  >19 group by item_id) c where b.item_id = c.item_id);
update aliItemTest b set b.V3 = (select c.cnt from      ##V3 - 每个商品收藏,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 group by item_id) c where b.item_id = c.item_id);
update aliItemTest b set b.V4 = (select c.cnt from      ##V4 - 每个商品购物车,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 group by item_id) c where b.item_id = c.item_id);
update aliItemTest b set b.V5 = (select c.cnt from      ##V5 - 每个商品购买,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 group by item_id) c where b.item_id = c.item_id);
update aliItemTest b set b.V6 = (select c.cnt from      ##V6 - 每个商品前一天浏览数
(select item_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H  >19 and D_H >= 48 group by item_id) c where b.item_id = c.item_id);
update aliItemTest b set b.V7 = (select c.cnt from      ##V7 - 每个商品前一天收藏,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 and D_H >= 48 group by item_id) c where b.item_id = c.item_id);
update aliItemTest b set b.V8 = (select c.cnt from      ##V8 - 每个商品前一天购物车,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 and D_H >= 48 group by item_id) c where b.item_id = c.item_id);
update aliItemTest b set b.V9 = (select c.cnt from      ##V9 - 每个商品前一天购买,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 and D_H >= 48 group by item_id) c where b.item_id = c.item_id);
update aliItemTest b set b.V10 = (select c.cnt from      ##V10 - 每个商品前3天浏览数
(select item_id,count(*) cnt from alimobrec where behavior_type = '1' and D_H  >19 and D_H >= 46 group by item_id) c where b.item_id = c.item_id);
update aliItemTest b set b.V11 = (select c.cnt from      ##V11 - 每个商品前3天收藏,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 and D_H >= 46 group by item_id) c where b.item_id = c.item_id);
update aliItemTest b set b.V12 = (select c.cnt from      ##V12 - 每个商品前3天购物车,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 and D_H >= 46 group by item_id) c where b.item_id = c.item_id);
update aliItemTest b set b.V13 = (select c.cnt from      ##V13 - 每个商品前3天购买,除去最后一天
(select item_id,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 and D_H >= 46 group by item_id) c where b.item_id = c.item_id);


#############################user-item pairs###################################
create table aliUserItemTest(
    user_item_pairs varchar(25),user_id varchar(25),item_id varchar(25),
    item_category varchar(25), user_category_pairs varchar(50),
    V1  float,  V2  float, V3  float, V4  float, V5  float, V6  float,
    V7  float,  V8  float, V9  float, V10  float,V11  float,V12  float,
	V13  float, V14  float,V15  float,V16  float,V17 float,V18 float,
    V19 float, V20 float, V21 float, V22 float, V23 float, V24 float,
    V25 float, V26 float, V27 float, V28 float, V29 float, V30 float
    );

insert into aliUserItemTest(user_item_pairs) select distinct(user_item_pairs) from alimobrec;

update aliUserItemTest b set b.V1 = (select c.cnt from      ##V1 - 每个用户-商品对记录数,除去最后一天
(select user_item_pairs,count(user_item_pairs) cnt from alimobrec where D_H  >19 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.V2 = (select c.cnt from      ##V2 - 每个用户-商品对浏览数,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H  >19 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.V3 = (select c.cnt from      ##V3 - 每个用户-商品对收藏,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.V4 = (select c.cnt from      ##V4 - 每个用户-商品对购物车,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.V5 = (select c.cnt from      ##V5 - 每个用户-商品对购买,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.V6 = (select c.cnt from      ##V6 - 每个用户-商品对前一天浏览数
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H  >19 and D_H >= 48 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.V7 = (select c.cnt from      ##V7 - 每个用户-商品对前一天收藏,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 and D_H >= 48 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.V8 = (select c.cnt from      ##V8 - 每个用户-商品对前一天购物车,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 and D_H >= 48 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.V9 = (select c.cnt from      ##V9 - 每个用户-商品对前一天购买,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 and D_H >= 48 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.V10 = (select c.cnt from      ##V10 - 每个用户-商品对前3天浏览数
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H  >19 and D_H >= 46 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.V11 = (select c.cnt from      ##V11 - 每个用户-商品对前3天收藏,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 and D_H >= 46 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.V12 = (select c.cnt from      ##V12 - 每个用户-商品对前3天购物车,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 and D_H >= 46 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.V13 = (select c.cnt from      ##V13 - 每个用户-商品对前3天购买,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 and D_H >= 46 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.V14 = (select c.cnt from      ##V14 - 每个用户-商品对前7天浏览数
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H  >19 and D_H >= 42 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.V15 = (select c.cnt from      ##V15 - 每个用户-商品对前7天收藏,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 and D_H >= 42 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.V16 = (select c.cnt from      ##V16 - 每个用户-商品对前7天购物车,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 and D_H >= 42 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.V17 = (select c.cnt from      ##V17 - 每个用户-商品对前7天购买,除去最后一天
(select user_item_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 and D_H >= 42 group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);

###########################category-features  Test####################################################
create table alicategoryTest(
    item_category varchar(25),
    V1  float,  V2  float, V3  float, V4  float, V5  float, V6  float,
    V7  float,  V8  float, V9  float, V10  float,V11  float,V12  float,
	V13  float, V14  float,V15  float,V16  float,V17 float,V18 float,
    V19 float, V20 float, V21 float, V22 float, V23 float, V24 float,
    V25 float, V26 float, V27 float, V28 float, V29 float, V30 float
    );

insert into alicategoryTest(item_category) select distinct(item_category) from alimobrec;
update alicategoryTest b set b.V1 = (select c.cnt from      ##V1 - 每个类别记录数,除去最后一天
(select item_category,count(item_category) cnt from alimobrec where D_H  >19 group by item_category) c where b.item_category = c.item_category);
update alicategoryTest b set b.V2 = (select c.cnt from      ##V2 - 每个类别浏览数,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '1' and D_H  >19 group by item_category) c where b.item_category = c.item_category);
update alicategoryTest b set b.V3 = (select c.cnt from      ##V3 - 每个类别收藏,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 group by item_category) c where b.item_category = c.item_category);
update alicategoryTest b set b.V4 = (select c.cnt from      ##V4 - 每个类别购物车,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 group by item_category) c where b.item_category = c.item_category);
update alicategoryTest b set b.V5 = (select c.cnt from      ##V5 - 每个类别购买,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 group by item_category) c where b.item_category = c.item_category);
update alicategoryTest b set b.V6 = (select c.cnt from      ##V6 - 每个类别前一天浏览数
(select item_category,count(*) cnt from alimobrec where behavior_type = '1' and D_H  >19 and D_H >= 48 group by item_category) c where b.item_category = c.item_category);
update alicategoryTest b set b.V7 = (select c.cnt from      ##V7 - 每个类别前一天收藏,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 and D_H >= 48 group by item_category) c where b.item_category = c.item_category);
update alicategoryTest b set b.V8 = (select c.cnt from      ##V8 - 每个类别前一天购物车,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 and D_H >= 48 group by item_category) c where b.item_category = c.item_category);
update alicategoryTest b set b.V9 = (select c.cnt from      ##V9 - 每个类别前一天购买,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 and D_H >= 48 group by item_category) c where b.item_category = c.item_category);
update alicategoryTest b set b.V10 = (select c.cnt from      ##V10 - 每个类别前3天浏览数
(select item_category,count(*) cnt from alimobrec where behavior_type = '1' and D_H  >19 and D_H >= 46 group by item_category) c where b.item_category = c.item_category);
update alicategoryTest b set b.V11 = (select c.cnt from      ##V11 - 每个类别前3天收藏,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 and D_H >= 46 group by item_category) c where b.item_category = c.item_category);
update alicategoryTest b set b.V12 = (select c.cnt from      ##V12 - 每个类别前3天购物车,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 and D_H >= 46 group by item_category) c where b.item_category = c.item_category);
update alicategoryTest b set b.V13 = (select c.cnt from      ##V13 - 每个类别前3天购买,除去最后一天
(select item_category,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 and D_H >= 46 group by item_category) c where b.item_category = c.item_category);


#############################user-category pairs  Test###################################
create table aliusercategoryTest(
    user_category_pairs varchar(25),
    V1  float,  V2  float, V3  float, V4  float, V5  float, V6  float,
    V7  float,  V8  float, V9  float, V10  float,V11  float,V12  float,
	V13  float, V14  float,V15  float,V16  float,V17 float,V18 float,
    V19 float, V20 float, V21 float, V22 float, V23 float, V24 float,
    V25 float, V26 float, V27 float, V28 float, V29 float, V30 float
    );

insert into aliusercategoryTest(user_category_pairs) select distinct(user_category_pairs) from alimobrec;
update aliusercategoryTest b set b.V1 = (select c.cnt from      ##V1 - 每个用户-类别对记录数,除去最后一天
(select user_category_pairs,count(user_category_pairs) cnt from alimobrec where D_H  >19 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliusercategoryTest b set b.V2 = (select c.cnt from      ##V2 - 每个用户-类别对浏览数,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H  >19 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliusercategoryTest b set b.V3 = (select c.cnt from      ##V3 - 每个用户-类别对收藏,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliusercategoryTest b set b.V4 = (select c.cnt from      ##V4 - 每个用户-类别对购物车,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliusercategoryTest b set b.V5 = (select c.cnt from      ##V5 - 每个用户-类别对购买,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliusercategoryTest b set b.V6 = (select c.cnt from      ##V6 - 每个用户-类别对前一天浏览数
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H  >19 and D_H >= 48 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliusercategoryTest b set b.V7 = (select c.cnt from      ##V7 - 每个用户-类别对前一天收藏,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 and D_H >= 48 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliusercategoryTest b set b.V8 = (select c.cnt from      ##V8 - 每个用户-类别对前一天购物车,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 and D_H >= 48 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliusercategoryTest b set b.V9 = (select c.cnt from      ##V9 - 每个用户-类别对前一天购买,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 and D_H >= 48 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliusercategoryTest b set b.V10 = (select c.cnt from      ##V10 - 每个用户-类别对前3天浏览数
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H  >19 and D_H >= 46 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliusercategoryTest b set b.V11 = (select c.cnt from      ##V11 - 每个用户-类别对前3天收藏,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 and D_H >= 46 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliusercategoryTest b set b.V12 = (select c.cnt from      ##V12 - 每个用户-类别对前3天购物车,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 and D_H >= 46 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliusercategoryTest b set b.V13 = (select c.cnt from      ##V13 - 每个用户-类别对前3天购买,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 and D_H >= 46 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliusercategoryTest b set b.V14 = (select c.cnt from      ##V14 - 每个用户-类别对前7天浏览数
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '1' and D_H  >19 and D_H >= 42 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliusercategoryTest b set b.V15 = (select c.cnt from      ##V15 - 每个用户-类别对前7天收藏,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '2' and D_H  >19 and D_H >= 42 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliusercategoryTest b set b.V16 = (select c.cnt from      ##V16 - 每个用户-类别对前7天购物车,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '3' and D_H  >19 and D_H >= 42 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);
update aliusercategoryTest b set b.V17 = (select c.cnt from      ##V17 - 每个用户-类别对前7天购买,除去最后一天
(select user_category_pairs,count(*) cnt from alimobrec where behavior_type = '4' and D_H  >19 and D_H >= 42 group by user_category_pairs) c where b.user_category_pairs = c.user_category_pairs);


#################combination Test################
update aliUserItemTest b set b.user_id = (select user_id from
(select user_id,user_item_pairs from aliMobrec group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.item_id  = (select item_id from
(select item_id,user_item_pairs from alimobrec group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.item_category  = (select item_category from
(select item_category,user_item_pairs from alimobrec group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);
update aliUserItemTest b set b.user_category_pairs  = (select user_category_pairs from
(select user_category_pairs,user_item_pairs from alimobrec group by user_item_pairs) c where b.user_item_pairs = c.user_item_pairs);


alter table aliUserTest add index INDEX_u(user_id);
alter table aliItemTest add index INDEX_i(item_id);
alter table aliCategoryTest add index INDEX_c(item_category);
alter table aliUserCategoryTest add index INDEX_uc(user_category_pairs);
alter table aliUserItemTest add index INDEX_ui(user_item_pairs);
alter table aliUserItemTest add index INDEX_uc (user_category_pairs);
alter table aliUserItemTest add index INDEX_u (user_id);
alter table aliUserItemTest add index INDEX_i (item_id);
alter table aliUserItemTest add index INDEX_c (item_category);

##################方法1###################
create table Testtmp1 as (select 
b.user_item_pairs,b.user_id,b.item_id,b.item_category,b.user_category_pairs,b.V1 as V1b,b.V2 as v2b,b.V3 as v3b,b.V4 as v4b,b.V5 as v5b,b.V6 as v6b,b.V7 as v7b,b.V8 as v8b,
b.V9 as v9b,b.v10 as v10b,b.v11 as v11b,b.v12 as v12b,b.v13 as v13b,b.v14 as v14b,b.v15 as v15b,b.v16 as v16b,b.v17 as v17b,
c.V1 as v1c,c.v2 as v2c,c.v3 as v3c,c.v4 as v4c,c.v5 as v5c,c.v6 as v6c,c.v7 as v7c,c.v8 as v8c,c.v9 as v9c,c.v10 as v10c,c.v11 as v11c,c.v12 as v12c,c.v13 as v13c
from aliUserItemTest b,aliUserTest c
where  b.user_id = c.user_id);

alter table Testtmp1 add index INDEX_c(item_category);

create table Testtmp2 as (select 
a.*,d.V1 as v1d,d.v2 as v2d,d.v3 as v3d,d.v4 as v4d,d.v5 as v5d,d.v6 as v6d,d.v7 as v7d,d.v8 as v8d,d.v9 as v9d,d.v10 as v10d,d.v11 as v11d,d.v12 as v12d,d.v13 as v13d
from Testtmp1 a,aliCategoryTest d
where a.item_category = d.item_category);

alter table Testtmp2 add index INDEX_uc(user_category_pairs);

create table Testtmp3 as (select
a.*,e.V1 as v1e,e.v2 as v2e,e.v3 as v3e,e.v4 as v4e,e.v5 as v5e,e.v6 as v6e,e.v7 as v7e,e.v8 as v8e,e.v9 as v9e,e.v10 as v10e,e.v11 as v11e,e.v12 as v12e,e.v13 as v13e,e.v14 as v14e,e.v15 as v15e,e.v16 as v16e,e.v17 as v17e
from Testtmp2 a,aliUserCategoryTest e
where a.user_category_pairs = e.user_category_pairs);

alter table Testtmp3 add index INDEX_i(item_id);

create table Test as (select
a.*,f.V1 as v1f,f.v2 as v2f,f.v3 as v3f,f.v4 as v4f,f.v5 as v5f,f.v6 as v6f,f.v7 as v7f,f.v8 as v8f,f.v9 as v9f,f.v10 as v10f,f.v11 as v11f,f.v12 as v12f,f.v13 as v13f
from Testtmp3 a,aliItemTest f
where a.item_id = f.item_id);

drop table testtmp1;
drop table testtmp2;
drop table testtmp3;


#####采用官方提供预测item集##############
create table aliItemPred(
    item_id  varchar(25),
    item_goehash varchar(15),
    item_category varchar(25)
    );

load data infile 'C:/b.csv' into table aliItemPred
fields terminated by ','  
optionally enclosed by '"' 
escaped by '"' 
lines terminated by '\n'
ignore 1 lines;

alter table aliItemPred add index INDEX_i(item_id);
alter table test add index INDEX_i(item_id);
alter table train add index INDEX_tgt(target);

create table testForSubmit as
(select * from test a where a.item_id in (select item_id from aliItemPred));

############################end########################################



##########################################################################################
select * from aliUser;
select * from aliItem;
select * from aliUserItem;
select * from aliCategory;
select * from alimobrec;
select * from train;
select * from test;
select * from testForSubmit;
select count(*) from train;
select count(*) from test;
select count(*) from testForSubmit;






