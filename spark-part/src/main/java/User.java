import java.io.Serializable;

public class User implements Serializable{
	private String email;
	private int age;
	private int followers_count;
	private String cat_max_post_count;
	private String cat_max_avg_post_likes;

	public String getEmail(){
		return this.email;
	}

	public void setEmail(String email){
		this.email = email;
	}

	public int getAge(){
		return this.age;
	}

	public void setAge(int age){
		this.age = age;
	}

	public int getFollowersCount(){
		return this.followers_count;
	}

	public void setFollowersCount(int followers_count){
		this.followers_count = followers_count;
	}

	public String getCatMaxPostCount(){
		return this.cat_max_post_count;
	}

	public void setCatMaxPostCount(String cat_max_post_count){
		this.cat_max_post_count = cat_max_post_count;
	}

	public String getCatMaxAvgPostLikes(){
		return this.cat_max_avg_post_likes;
	}

	public void setCatMaxAvgPostLikes(String cat_max_avg_post_likes){
		this.cat_max_avg_post_likes = cat_max_avg_post_likes;
	}
}