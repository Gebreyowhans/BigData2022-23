package models;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class PostsHBaseModel {
    public static final TableName TABLE_POSTS = TableName.valueOf("posts");
    public static final byte[] POST_ID = Bytes.toBytes("post-id");
    public static final byte[] DATE = Bytes.toBytes("Date");
    public static final byte[] POST_INFO_FAMILY= Bytes.toBytes("info");
    public static final byte[] POST_CATEGORY_QUALIFIER = Bytes.toBytes("category");
    public static final byte[] POST_USER_EMAIL_QUALIFIER = Bytes.toBytes("user-email");
    public static final byte[] POST_LIKES_FAMILY= Bytes.toBytes("likes");
    public static final byte[] USERS_QUALIFIER = Bytes.toBytes("users");
    public static final byte[] NUMBER_OF_LIKES_QUALIFIER = Bytes.toBytes("number-of-likes");
}
