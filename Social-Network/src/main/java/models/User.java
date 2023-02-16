package models;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class User implements WritableComparable<User> {

    String email;
    String fullName;
    Integer age;
    Integer followers_Count;

    public User() {
        this.email = "";
        this.fullName = "";
        this.age = 0;
        this.followers_Count = 0;
    }

    public User(String email, String fullName, Integer age, Integer followers_Count) {
        this.email = email;
        this.fullName = fullName;
        this.age = age;
        this.followers_Count = followers_Count;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Integer getFollowers_Count() {
        return followers_Count;
    }

    public void setFollowers_Count(Integer followers_Count) {
        this.followers_Count = followers_Count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(email);
        out.writeUTF(fullName);
        out.writeInt(age);
        out.writeInt(followers_Count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        email = in.readUTF();
        fullName = in.readUTF();
        age = in.readInt();
        followers_Count = in.readInt();
    }

    @Override
    public int compareTo(User user) {
        return email.compareTo(user.email);
    }

}
