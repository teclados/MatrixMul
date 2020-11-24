import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 

public class MatrixMul {
	public static class MatrixMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String temp = line.substring(1, line.length()-1);
			String[] record = temp.split(", ");
			Text key1 = new Text();
			Text value1 = new Text();
			String label = record[0].substring(1, record[0].length()-1);
			if(label.equals("a")){
				for(int i=0; i<5; i++){
					key1.set(record[1] + "," + i);
					value1.set("a"+","+record[2]+","+record[3]);
					context.write(key1, value1);
				}
			}
			else{
				for(int i=0; i<5; i++){
					key1.set(i + "," + record[2]);
					value1.set("b"+","+record[1]+","+record[3]);
					context.write(key1, value1);
				}
			}
		}
	}
	
	public static class MultiplyReducer extends Reducer<Text, Text, Text, IntWritable>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String[] value;
			int result = 0;
			HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
			HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
			for(Text record:values){
				value = record.toString().split(",");
				if(value[0].equals("a")){
					hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
				}
				else{
					hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
				}
			}
			float mul_value = 0.0f;
			float a_value = 0;
			float b_value = 0;
			for(int i=0 ; i<5 ; i++){
				a_value = hashA.containsKey(i) ? hashA.get(i) : 0.0f;
				b_value = hashB.containsKey(i) ? hashB.get(i) : 0.0f;
				mul_value = a_value*b_value;
				result += mul_value;
			}
			context.write(key, new IntWritable(result));
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "multiply matrix");
		job.setJarByClass(MatrixMul.class);
		job.setMapperClass(MatrixMapper.class);
		job.setReducerClass(MultiplyReducer.class);
		job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)? 0: 1);
	}
}
