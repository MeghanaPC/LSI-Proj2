import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MainClass {

	public static enum MRCounter
	{
		RESIDUAL;
	}
	public static final long  numNodes=685230;
	public static final int numIterations=2;
	public static final double epsilon=0.001;
	public static final int precision=100000;
	
	 public static void main(String[] args) throws Exception {
		   Configuration conf = new Configuration();
		   double residual_error=9999999.9;
		   int count=0;
		   while(count<numIterations)
		   {
			   
			   Job job = new Job(conf, "MainClass"+count);
			   job.setOutputKeyClass(Text.class);
			   job.setOutputValueClass(Text.class);
			       
			   job.setMapperClass(NormalMapper.class);
			   job.setReducerClass(NormalReducer.class);
			       
			   job.setInputFormatClass(TextInputFormat.class);
			   job.setOutputFormatClass(TextOutputFormat.class);
			   
			   if(count==0)
			   {
				   FileInputFormat.addInputPath(job, new Path("inputDir"));
				   
			   }
			   else
			   {
				   FileInputFormat.addInputPath(job, new Path("output/file"+(count-1)));
			   }
			   FileOutputFormat.setOutputPath(job, new Path("output/file"+count));
			       
			   job.waitForCompletion(true);
			   
			   long residual=job.getCounters().findCounter(MRCounter.RESIDUAL).getValue();
			   residual_error=(double)(residual/precision)/numNodes;
			   
			   System.out.println("Iteration : "+count+"-------"+"Residual : "+residual_error);
			   job.getCounters().findCounter(MRCounter.RESIDUAL).setValue(0L);
			   ++count;
		   }
		       
		       
		   
		}
}
