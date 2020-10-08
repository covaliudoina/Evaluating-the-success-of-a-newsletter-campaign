
CREATE DATABASE Newsletter;

USE Newsletter;


# Creating and populating Content table
CREATE TABLE Content(
content_id VARCHAR(255),
polopoly_event_type TEXT,
polopoly_event_time TIMESTAMP,
polopoly_department text,
content_word_count int,
content_related_link_count int,
content_media_count int,
content_image_count int,
content_comment_flag BOOLEAN,
content_label text,
content_headline text,
content_path text,
content_type text,
content_subject text,
content_person text,
content_tag text,
load_dt date,
insert_tmstmp TIMESTAMP,
update_tmstmp TIMESTAMP,
unique(content_id,polopoly_event_time));

DESCRIBE Content;

# TO BE ABLE TO LOAD THE CSV FILE INTO THE TABLE CREATE ABOVE FEW CHANGES HAD TO BE DONE:
# The CSV format for the timestamp is not reconized in MySql. I trimed the field to fit into the 
# format  yyyy-mm-dd hh:mm:ss
# the blank fields in the csv file are not regonized as a valid integer value and were tranformed into NULL
# before loading the values in the table

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/dataset.csv'
REPLACE
INTO TABLE Content
FIELDS TERMINATED BY ','
Enclosed BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(content_id, polopoly_event_type, @var1, polopoly_department, 
@var2,@var3, @var4, 
@var5, @var6, content_label, content_headline, 
content_path,content_type, content_subject, content_person, content_tag, load_dt, 
@var7, @var8)
set polopoly_event_time = trim(substr(@var1,1,19)),
	content_word_count= if(@var2='',NULL,@var2),
    content_related_link_count= IF(@var3='',NULL, @var3),
    content_media_count=IF(@var4='', NULL, @var4),
    content_image_count=IF(@var5='',NULL, @var5),
    content_comment_flag=(@var6='TRUE'),
	insert_tmstmp=trim(substr(@var7,1,19)),
    update_tmstmp=IF( @var8='',NULL,@var8);
    

# Creating and populating the web TABLE


CREATE TABLE web
(session_id bigint,
event_type TEXT,
device_id TEXT,
referrer_url TEXT,
content_url TEXT,
content_id TEXT,
content_area TEXT,
event_time TIMESTAMP,
referrer_campaign TEXT,
content_embedded_media TEXT,
content_media_audio_video TEXT,
content_type TEXT,
user_tier TEXT);


# The header of the csv file apears few time in the dataset generating errors 
# as the string cannot be uploaded in a field that is defined as BIGINT. As a result the value
# "session_id" in the session_id field will be replaced by NULL while loading and can be later deleted 
# if we decide to clean the dataset.
 
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/web.csv'
INTO TABLE web
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS

(@var9, event_type, device_id, referrer_url, content_url, content_id, content_area,
@var10, referrer_campaign, content_embedded_media, content_media_audio_video, 
content_type, user_tier)
set 
session_id=if(@var9=('session_id'), NULL, @var9),
event_time=if(@var10=('event_time'), NULL, trim(substr(@var10,1,19)));

#Counting the rows in each table
select count(*) as number_of_rows from web ; 

select count(*) as number_of_rows from Content;


# 1: The top 10 headlines by page views for free, authenticated, and premium users

# I used a window querry along with the RANK function to extract the Top headline for each user type
# after joining the datasets web and Content. In the Content dataset the same content__id has 
# multiple headline as the same article was updated multiple times. For this analysis I assign to 
# each content_id the initial content headline

SELECT b.user_tier, b.content_headline, b.no_views 
FROM
(
SELECT a.user_tier, a.content_headline, a.no_views, 
rank() over (partition by user_tier order by no_views desc) as ranks 
from 
	(
    SELECT user_tier, content_headline, no_views 
    FROM 
		(
		select user_tier, event_type, content_id, count(*) as no_views 
        FROM web 
		group by content_id
		having event_type='LOADED' AND NOT content_id=''
		)  as web1
	join
		(
		select content_id, content_headline # content_id apears multiple times in Content DATASET 
		from Content 				#for the same article that was published and updated multiple times
		group by content_id
				) as k1
	on web1.content_id=k1.content_id
	) as a
) as b
where b.ranks<= 10;

 
#2 Average time spent per session for each of the three days in the newsletter campaign

#As the session_id contains the miliseconds from the start of the first event of the session, 
# i will calculate the average time per session using sesssion_id field
select date(event_time) as Dates, avg(session_id) AS AVG_TIME
from web
group by Dates
having Dates IS NOT NULL;


#3 How many page views can be attributed to Facebook on each of the three days of the campaign

#I considered that a page was viewed if it was LOADED and the view can be attributed 
# to facebook if the referrer_url contains the word "Facebook"

select date(event_time) as Dates, count(distinct device_id) as no_of_views
from web 
where event_type='LOADED' AND  referrer_url like '%Facebook%'
group by Dates;


