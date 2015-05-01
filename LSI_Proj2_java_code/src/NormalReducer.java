import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class NormalReducer extends Reducer<Text,Text,Text,Text> {
	
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		//val= nodeid ; pagerank ; #outgoingEdges ; outgoingEdgeList (delimited by ,)
		//val = partialPR
		Double damp=0.85;
		Long totalnodes=(long) 3;
		Double sumPR=0.0;
		Double oldPR=0.0;
		String outEdges="";
		for(Text val:values)
		{
			String input=val.toString();
			String inputarr[]=input.split("\\s+");
			
			if(inputarr.length>1)   //the original graph structure
			{
				oldPR=Double.parseDouble(inputarr[1]);
				outEdges=inputarr[3];
				
			}
			else   // the new page rank
			{
				sumPR+=Double.parseDouble(inputarr[0]);
			}
		}
		
		Double firstPart=(double)(1-damp)/totalnodes;
		Double newPR=firstPart+(damp*sumPR);
		
		long residual=(long) Math.abs(((oldPR-newPR)/newPR)*MainClass.precision);
		context.getCounter(MainClass.MRCounter.RESIDUAL).increment(residual);
		// add code to add residual to hadoop counter
		
		//String emitString=key.toString()+";"+newPR.toString()+";"+outEdges.split(",").length+";"+outEdges;
		String emitString=newPR.toString()+" "+outEdges.split(",").length+" "+outEdges;
		context.write(key, new Text(emitString));
		
	}
		
	
}
