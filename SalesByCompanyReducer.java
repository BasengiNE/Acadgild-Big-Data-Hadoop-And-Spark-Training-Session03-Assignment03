package TelevisionSales;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesByCompanyReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {

		int countByCompany = 0;
		
		while (values.hasNext()) {
			IntWritable value = (IntWritable) values.next();
			countByCompany += value.get();
		}
		output.collect(key, new IntWritable(countByCompany));
	}
}
