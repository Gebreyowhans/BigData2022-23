package models;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Likes implements WritableComparable<Likes> {
    String userEmail;
    String postId;

    public Likes() {
        this.userEmail = "";
        this.postId = "";
    }

    public Likes(String userEmail, String postId) {
        this.userEmail = userEmail;
        this.postId = postId;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public String getPostId() {
        return postId;
    }

    public void setPostId(String postId) {
        this.postId = postId;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userEmail);
        out.writeUTF(postId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userEmail = in.readUTF();
        postId = in.readUTF();
    }

    @Override
    public int compareTo(Likes o) {
        return postId.compareTo(o.postId);
    }
}
