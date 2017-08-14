import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBOutputWritable implements DBWritable{

	private String starting_phrase;
	private String following_word;
	private double relative_freq;
	
	public DBOutputWritable(String starting_phrase, String following_word, double relative_freq) {
		this.starting_phrase = starting_phrase;
		this.following_word = following_word;
		this.relative_freq = relative_freq;
	}

	public void readFields(ResultSet arg0) throws SQLException {
		this.starting_phrase = arg0.getString(1);
		this.following_word = arg0.getString(2);
		this.relative_freq = arg0.getDouble(3);
		
	}

	public void write(PreparedStatement arg0) throws SQLException {
		arg0.setString(1, starting_phrase);
		arg0.setString(2, following_word);
		arg0.setDouble(3, relative_freq);
		
	}

}
