
/***
 * OurReducer class
 * @author ahmed ferjani
 */

import java.io.IOException;

		import org.apache.hadoop.io.NullWritable;
		import org.apache.hadoop.io.Text;
		import org.apache.hadoop.mapreduce.Reducer;
		import java.util.Vector;


public class OurReducer extends Reducer<Text, Text, NullWritable, Text> {
	/*
		This function is our reducer
	 */

	//vector of strings containing values of the output:
	Vector<Vector<String>> out = new Vector<>();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// verify if mapper has finsished its job to get the final result and display it

		if(key.toString().equals("EOF")){

			for (int i = 0; i < out.size(); i++) {
				// ret is a string that contains the transpose of a csv file.
				//System.out.println(out.get(i));
				String ret = "";
				for (int j = 0; j < out.get(i).size(); j++) {
					ret += out.get(i).get(j) + ";";
				}

				//System.out.println(ret);  // uncomment the following line if you want to see results
				//Get value from context.write, no key is needed
				context.write(NullWritable.get(), new Text(ret));
			}
		}
		else {
			// fill the vector with results from mapper
			// (i.e: get the first value and assign it to the first vector, the second value to the second vector etc...
			int k = 0;
			for (Text t : values) {
				k++;
				while (out.size() <= k) {
					out.add(new Vector<String>());
				}
				//System.out.println(Integer.toString(k)+" " +t.toString());
				out.get(k).add(t.toString());
			}
		}
	}
}
