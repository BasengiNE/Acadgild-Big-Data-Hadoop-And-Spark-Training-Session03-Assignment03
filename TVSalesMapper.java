package TelevisionSales;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class TVSalesMapper extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {
	private final static IntWritable oneSale = new IntWritable(1);
	
	public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String line = value.toString();
		String[] tokens = line.split("|");
		//Ignore records which have NA for the first 2 fields
		if(!tokens[0].equalsIgnoreCase("NA") && !tokens[1].equalsIgnoreCase("NA"))
			output.collect(new Text(valueString), oneSale);
	}
}
