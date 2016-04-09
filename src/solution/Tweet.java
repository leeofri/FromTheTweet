package solution;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.annotate.JsonTypeInfo.As;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Tweet implements Writable, Comparable<Tweet> {

	private Text text;
	private IntWritable date;
	private IntWritable followersCount;
	private IntWritable location;
	private IntWritable followingCount;


	public Tweet(String text,int date,int followers,int location,int following) {
		
		this.setText(new Text(text));
		this.setDate(new IntWritable(date));
		this.setFollowersCount(new IntWritable(followers));
		this.setLocation(new IntWritable(location));
		this.setFollowingCount(new IntWritable(following));
	}

 
	// Getters \ Setters 
	
	private Text getText() {
		return text;
	}

	private void setText(Text text) {
		this.text = text;
	}



	private IntWritable getDate() {
		return date;
	}



	private void setDate(IntWritable date) {
		this.date = date;
	}



	private IntWritable getFollowersCount() {
		return followersCount;
	}



	private void setFollowersCount(IntWritable followersCount) {
		this.followersCount = followersCount;
	}



	private IntWritable getLocation() {
		return location;
	}



	private void setLocation(IntWritable location) {
		this.location = location;
	}



	private IntWritable getFollowingCount() {
		return followingCount;
	}



	private void setFollowingCount(IntWritable followingCount) {
		this.followingCount = followingCount;
	}

	// Metods
	
	@Override
	public void write(DataOutput out) throws IOException {
		
		// write all the file to the disk in the DM order
		this.getText().write(out);
		this.getText().write(out);
		this.getFollowersCount().write(out);
		this.getLocation().write(out);
		this.getFollowingCount().write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		try {
			// read all the files in the DM order
			this.getText().readFields(in);
			this.getText().readFields(in);
			this.getFollowersCount().readFields(in);
			this.getLocation().readFields(in);
			this.getFollowingCount().readFields(in);
			
		} catch (ClassCastException cce) {
			throw new IOException(cce);
		}
	}

	@Override
	public int compareTo(Tweet o) {
		return 0;
	}
	
	public static Tweet GetTweetFromLine(Text value) {
		// split the the line to name|days..
		String[] data = value.toString().split("~%");

		// create the tweet objact
		
		return null;
	}
}
