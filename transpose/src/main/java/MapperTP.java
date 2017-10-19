import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTP extends Mapper < Text, Text, LongWritable, Text > {
    // n counts the number of lines in a csv file
    private static int n = 0;

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
		/*
			ourmapper: <key: line number, value: content of the line>
		 */
        // split each value of each line
        String [] arr = key.toString().split(",");

        for (int i=0; i<arr.length;i++){
            // every value of each field of th ith line is associated to key=n
            context.write(new LongWritable(n),new Text(i + "/" + arr[i]));

        }
        n++;
    }

    @Override
    protected void cleanup (Context c) throws IOException{
		/*
			send a message to reduce to inform him that we have atteined the end of file
		 */
        try{
            c.write(new LongWritable(Long.MAX_VALUE),new Text(""));
        } catch (Exception e){
            e.printStackTrace();
        }
    }

}