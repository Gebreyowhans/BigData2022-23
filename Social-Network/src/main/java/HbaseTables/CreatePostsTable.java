package HbaseTables;

import models.PostsHBaseModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import utilities.Constants;

import java.io.IOException;

public class CreatePostsTable {
    public static void createTable() {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", Constants.HDFS_BASE_URL);
            conf.set("hbase.zookeeper.quorum", Constants.CLUSTER_MACHINES);
            try {
                Connection con = ConnectionFactory.createConnection(conf);
                ColumnFamilyDescriptorBuilder postInfoFamily = ColumnFamilyDescriptorBuilder.newBuilder(PostsHBaseModel.POST_INFO_FAMILY);
                ColumnFamilyDescriptorBuilder LikesFamily = ColumnFamilyDescriptorBuilder.newBuilder(PostsHBaseModel.POST_LIKES_FAMILY);
                TableDescriptorBuilder postsTable = TableDescriptorBuilder.newBuilder(PostsHBaseModel.TABLE_POSTS);
                postsTable.setColumnFamily(postInfoFamily.build());
                postsTable.setColumnFamily(LikesFamily.build());
                Admin admin = con.getAdmin();
                if (admin.tableExists(PostsHBaseModel.TABLE_POSTS)) {
                    System.out.println("Disabling table");
                    admin.disableTable(PostsHBaseModel.TABLE_POSTS);
                    System.out.println("Deleting table");
                    admin.deleteTable(PostsHBaseModel.TABLE_POSTS);
                }

                System.out.println("Creating table");
                admin.createTable(postsTable.build());
                admin.close();
                con.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
    }
}
