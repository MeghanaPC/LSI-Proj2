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
	//public static final long numNodes=3;
	public static final int numIterations=6;
	public static final double epsilon=0.001;
	public static final int precision=1000;      //can change precision as well
	
	 public static void main(String[] args) throws Exception {
		   Configuration conf = new Configuration();
		   double residual_error=0.0;
		   int count=0;
		   while(count<numIterations)     //right now until convergence. can change it to while count<numIterations
		   {
			   
			   Job job = new Job(conf, "MainClass"+count);
			   job.setJarByClass(MainClass.class);
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
			   //System.out.println("summed up residual::::"+residual);
			   
			   residual_error=(residual/numNodes)/(double)precision;
			   
			   String residualErrorString = String.format("%.4f", residual_error);
			   
			  // System.out.println("Iteration : "+count+"-------"+"Residual : "+residualErrorString);
			   job.getCounters().findCounter(MRCounter.RESIDUAL).setValue(0L);
			   ++count;
			   
			   
			   
		   }
		       
		       
		   
		}
}
