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
			ourmapper: <key: line number, value:(column_number, value)>
		 */
        // split each value of each line
        String [] arr = key.toString().split(",");

        for (int i=0; i<arr.length;i++){
            // key = number of line
	    // value (column_number, value_Of_Column)
            context.write(new LongWritable(n),new Text(i + "/" + arr[i]));

        }
        n++;
    }

    @Override
    protected void cleanup (Context c) throws IOException{
		/*
			send a message to reducer to inform him that we have reached the end of file.
		 */
        try{
            c.write(new LongWritable(Long.MAX_VALUE),new Text(""));
        } catch (Exception e){
            e.printStackTrace();
        }
    }

}
