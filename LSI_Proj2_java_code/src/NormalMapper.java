import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class NormalMapper extends Mapper<LongWritable,Text,Text,Text>{
	//file format nodeid  pagerank  #outgoingEdges  outgoingEdgeList (delimited by ,)
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String input=value.toString();
		String inputarr[]=input.split("\\s+");
		String nodeID=inputarr[0];
		Double nodePR=new Double(inputarr[1]);
		Integer nodeDegree=new Integer(inputarr[2]);
		String nodeEdges="";
		if(inputarr.length==4)
		{
			nodeEdges=inputarr[3];
		}
		
		
		context.write(new Text(nodeID), new Text(input));
		
		//System.out.println("input passed to mapper-------"+input);
		if(nodeEdges!="")
		{
			Double newPR=nodePR/nodeDegree;    //won't be 0 if nodeEdges!=""
			String edgeList[]=nodeEdges.split(",");
			for(String edgenode:edgeList)
			{
				context.write(new Text(edgenode),new Text(newPR.toString()));
			}
			
		}
			
		
		
	}
}
