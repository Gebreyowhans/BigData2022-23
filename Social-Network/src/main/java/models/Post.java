package models;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class Post implements WritableComparable<Post> {
    String postId;
    String category;
    String date;
    String user_Email;

    public Post() {
        this.postId = "";
        this.category = "";
        this.date = "";
        this.user_Email = "";
    }

    public Post(String postId, String category, String date,
                String user_Email) {
        this.postId = postId;
        this.category = category;
        this.date = date;
        this.user_Email = user_Email;
    }

    public String getPostId() {
        return postId;
    }

    public void setPostId(String postId) {
        this.postId = postId;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getUser_Email() {
        return user_Email;
    }

    public void setUser_Email(String user_Email) {
        this.user_Email = user_Email;
    }

    @Override
    public int compareTo(Post post) {
        return postId.compareTo(post.postId);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(postId);
        out.writeUTF(category);
        out.writeUTF(date);
        out.writeUTF(user_Email);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        postId = in.readUTF();
        category = in.readUTF();
        date = in.readUTF();
        user_Email = in.readUTF();
    }
}
