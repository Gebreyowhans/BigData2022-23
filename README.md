# 05-project-socialnetwork-project-socialnetwork

## Problem 1 (Sqoop - Hive)

### Creating Mysql database,tables and loading the csv data into tables

#### 1, create database;
          create database SocialNetwork;

#### 2,Create user table
         create table USER (email VARCHAR(255) NOT NULL PRIMARY KEY,fullname VARCHAR(255) NOT NULL,age INT NOT NULL,followers_count INT NOT NULL);
#### 3,create post table
         create table POST (post_id INT NOT NULL PRIMARY KEY,category VARCHAR(255) NOT NULL,date DATE NOT NULL,user_email VARCHAR(255) NOT NULL,FOREIGN KEY
         (user_email) REFERENCES USER(email));
      
#### 4,create likes table
       create table LIKES (user_email VARCHAR(255) NOT NULL,post_id INT NOT NULL,PRIMARY KEY (user_email, post_id),FOREIGN KEY (user_email) REFERENCES USER(email)
       FOREIGN KEY (post_id) REFERENCES POST(post_id));
      
#### 5, load users.csv data into user table
       load data local infile 'users.csv' into table USER fields terminated by ',';

#### 6,load posts.csv into post table 
      load data local infile 'posts.csv' into table POST fields terminated by ',';

#### 7,load likes.csv into likes table 
      load data local infile 'likes.csv' into table LIKES fields terminated by ',';
#### 8,Import all tables into warhouse directory using sqoop
      sqoop-import-all-tables --connect jdbc:mysql://master/SocialNetwork --username gebre -P --warehouse-dir /home/gebre/SocialNetwork -m 1


## Problem 2 (HBase - Map Reduce)
### Task 1 : Design a MapReduce job that joins post.csv and likes.csv into an HBase table (posts)
 To solve this problem we used two maper classes and one reducer class :
 
  1.__PostsMapper__ which is responsible for parsing posts.csv file and emit into hdfs file system using __post id__ as key and __category__,
     __date__,__useremail__ as values and here we also included a tag named __post__ at the begining of the values string which will 
     further be used as an identifer to Post or likes in the reducer side.
     
  2.__LikesMapper__ which is responsible for parsing likes.csv file and emit into hdfs file system using __post id__ as key and 
     __useremail__(a user who likes that post id) as  value and here we also included a tag named __Likes__ at the begining of the
     values string which will further be used as an identifer to post or likes in the reducer side. The image attached in the below shows the
     output of the two mappers classes(i.e tha data which is ready to be sent to the reducer)     
        ![image](https://user-images.githubusercontent.com/34910065/210007161-7cfa8ee3-8cfa-404b-a6b6-cff00fb62df2.png)
        
  3. The mappers will finally send list of __PostId__ keys and __list of users__ who liked the postid ,__date__ the post is posted ,
     the __post category__ and the __user__ who posts  the post to reducer class.
     
  4. The __PostLikesReducer__ then receives the data from the mappers and make some parsing to collect all the users 
     who liked a particular postid into a __json array__ object,uses __postid_date__ as row key ,__info__ as column family 1 
     with category and user email as column qualifier,likes column family with users qualifier 
     (stores list of users who liked the post id in json array format),__numberoflikes qualifier we created it by ourselfs 
      to store the number of users who liked the post becausse this column will help us in the second task__
      and finally this reducer stores this all info into an hbase table named __posts__.
      
  ### Task 2 : Design a MapReduce job that reads from the previous HBase table and computes:
  #### 1.Count of posts per category posted by a user
  #### 2.Average number of likes obtained by posts per category of each users
  
  ##### Solution:
  To solve this problem we used __one maper,one combiner and one reducer class__ where :
  
  1. The maper class named __PostCountAndAvgLikesMapper__ reads data from __Hbase__ table and extracts the 
     __post category__,__user email__ who made post and __number of likes__.  Then Constructed a composite key using the __user ID__
     and __category__ and we emit the __composite key__ as a key  and the __number of likes__ as the value and send this data to the combiner 
     to perform map side aggregation. The result of this maper looks like as in the below image.
     ![image](https://user-images.githubusercontent.com/34910065/210009473-ea1124c1-3c4e-4c77-be0e-bfff367cd44b.png)
     
  2. The combiner classe named __PostCountAndAvgLikesCombiner__ takes the key value paires obtained from the previous maper and groups all 
     post categories which have  the same composite key and summs the number of likes obtained for each category and produces a new key value pair 
     that has count of all posts of the same category per user and sum of likes for each post category  and the same composite key too. The result of
     the combiner class looks like in the below image.
     
 ![image](https://user-images.githubusercontent.com/34910065/210010469-a4b0e305-d7bb-4968-ad03-062856c5a4bc.png)
 
 3. The Reducer class named __PostCountAndAvgLikesReducer__ the receives the aggragated count of posts and total number of likes 
    obtained per every user post and category and then computes the average number of likes obtained using  
    __Average = total likes obtained per post category of user/count of post category__. Then it will finally emits, 
    the __Composite key__ as a key , __count of posts__ ,__number of likes__ ,__Average likes__ obtained as values.
    The output of the reducer will look like as in the below image.
    
![image](https://user-images.githubusercontent.com/34910065/210011352-53744f37-b47f-4690-896e-d56b2c107167.png)
      
  

## Problem 3 (Spark)
  
  
     
        
       
