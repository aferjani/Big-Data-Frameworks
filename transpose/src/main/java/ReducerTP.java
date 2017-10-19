import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerTP extends Reducer< LongWritable, Text, NullWritable, Text > {
    /*
		This function is our reducer
	 */

    //vector of strings containing values of the output:
    Vector<Vector<String>> out = new Vector<Vector<String>>();

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // verify if mapper has finsished its job to get the final result and display it
        //System.out.println("Key : " + key);
        //System.out.println("Values : ");
        //for (Text t : values)
        //    System.out.print(t + " ");
        //System.out.println();
        if(key.get() == Long.MAX_VALUE){
            for (int i = 0; i < out.size(); i++) {
                // ret is a string that contains the transpose of a csv file.
                //System.out.println(out.get(i));
                String ret = "";
                for (int j = 0; j < out.get(i).size() - 1; j++) {
                    ret += out.get(i).get(j) + ",";
                }
                ret += out.get(i).get(out.get(i).size() - 1);

                System.out.println(ret);  // uncomment the following line if you want to see results
                //Get value from context.write, no key is needed
                //System.out.println(ret);
                context.write(NullWritable.get(), new Text(ret));
            }
        } else {
            // fill the vector with results from mapper
            // (i.e: get the first value and assign it to the first vector, the second value to the second vector etc...

            for (Text t : values) {
                String[] arr = t.toString().split("/");
                int k = Integer.parseInt(arr[0]);
                while (out.size() <= k) {
                    out.add(new Vector<String>());
                }

                out.get(k).add(arr[1]);
            }
        }
    }
}