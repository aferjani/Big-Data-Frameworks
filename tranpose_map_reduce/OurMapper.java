/***
 * OurMapper class
 * @author ahmed ferjani
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OurMapper extends Mapper<Text, Text, Text, Text> {
	// n counts the number of lines in a csv file
	int n = 0;

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		/*
			ourmapper: <key: line number, value: content of the line>
		 */
		// split each value of each line
		String [] arr = value.toString().split(";");

		for (int i=0; i<arr.length;i++){
			// every value of each field of th ith line is associated to key=n
			context.write(new Text(Integer.toString(n)),new Text(arr[i]));

		}
		n++;
	}

	@Override
	protected void cleanup (Context c) throws IOException{
		/*
			get result after the map finishes its job
		 */
		try{
			c.write(new Text("EOF"),new Text(""));
		}
		catch (Exception e){

		}

	}
}
