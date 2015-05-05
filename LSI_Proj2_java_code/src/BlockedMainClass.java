import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class BlockedMainClass {

	public static enum MRCounter
	{
		RESIDUAL;
	}
	public static final long  numNodes=685230;
	//public static final long numNodes=3;
	public static final int numIterations=9;
	public static final double epsilon=0.001;
	public static final int precision=10000;      //can change precision as well
	public static final String NODEINFO = "NODEINFO";
	public static final int blockSize=10000;      //can change precision as well

	
	public static int blockNumberArray[];
		
	 public static void main(String[] args) throws Exception {
		   Configuration conf = new Configuration();
		   double residual_error=1.0;
		   int count=0;
		   
		   blockNumberArray = new int[68];
		   int index = 0;
		   int prev = 0;

		   BufferedReader blockFileReader = new BufferedReader(new FileReader(args[2]));
		   String line = null;
		   while((line = blockFileReader.readLine()) != null){
			   if(index > 0){
				   prev = blockNumberArray[index-1];
			   }
			   blockNumberArray[index] = prev + Integer.parseInt(line.trim());
			   index++;
		   }
		   
		   blockFileReader.close();
		   
		   BufferedWriter writer = new BufferedWriter(new FileWriter("residuals.txt"));

		  // while(count<numIterations)     //right now until convergence. can change it to while count<numIterations
		   while(residual_error >= epsilon)
		   {
			   
			   Job job = new Job(conf, "BlockedMainClass"+count);
			   job.setJarByClass(BlockedMainClass.class);
			   job.setOutputKeyClass(Text.class);
			   job.setOutputValueClass(Text.class);
			       
			   job.setMapperClass(BlockMapper.class);
			   job.setReducerClass(BlockReducer.class);
			       
			   job.setInputFormatClass(TextInputFormat.class);
			   job.setOutputFormatClass(TextOutputFormat.class);
			   
			   if(count==0)
			   {
				   FileInputFormat.addInputPath(job, new Path(args[0]));
				   
			   }
			   else
			   {
				   FileInputFormat.addInputPath(job, new Path(args[1] + "/file"+(count-1)));
			   }
			   FileOutputFormat.setOutputPath(job, new Path(args[1] + "/file"+count));
			       
			   job.waitForCompletion(true);
			    
			   long residual=job.getCounters().findCounter(MRCounter.RESIDUAL).getValue();
			   //System.out.println("summed up residual::::"+residual);
			   
			   residual_error=(residual/numNodes)/(double)precision;
			   
			   String residualErrorString = String.format("%.4f", residual_error);
			   
			   writer.write("Iteration : "+count+"-------"+"Residual : "+residualErrorString);
			   writer.newLine();
			  // System.out.println("Iteration : "+count+"-------"+"Residual : "+residualErrorString);
			   job.getCounters().findCounter(MRCounter.RESIDUAL).setValue(0L);
			   ++count;			   
			   
		   }
		   writer.close();
		   
		}
}
