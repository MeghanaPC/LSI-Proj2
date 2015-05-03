import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class BlockMapper extends Mapper<LongWritable,Text,Text,Text>{
	//file format blockid nodeid pagerank #outgoingEdges outgoingEdgeList (delimited by ,)
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String input=value.toString();
		String inputarr[]=input.split("\\s+");
		String nodeID=inputarr[1];
		String blockID = inputarr[0];
		Double nodePR=new Double(inputarr[2]);
		Integer nodeDegree=new Integer(inputarr[3]);
		String nodeEdges="";
		if(inputarr.length==5)
		{
			nodeEdges=inputarr[4];
		}
		
		String edgeListString = "";
		
		//System.out.println("input passed to mapper-------"+input);
		if(nodeEdges!="")
		{
			Double newPR=nodePR/nodeDegree;    //won't be 0 if nodeEdges!=""
			String edgeList[]=nodeEdges.split(",");
			for(String edgenode:edgeList)
			{
				String neighborBlockID = blockIDFromNodeID(edgenode);
				edgeListString += neighborBlockID + ":" + edgenode + ",";
				context.write(new Text(neighborBlockID),new Text(edgenode + " " + blockID + " " + newPR.toString()));
			}
			
		} 
		//Remove last comma
		edgeListString = edgeListString.substring(0,edgeListString.length()-1);
		context.write(new Text(blockID), new Text(MainClass.NODEINFO + " " + nodeID + " " + nodePR + " " + edgeListString));

		
	}

	private String blockIDFromNodeID(String edgenode) {
		//Dummy implementation:
		if(edgenode.equals("0") || edgenode.equals("1")){
			return "0";
		}else if(edgenode.equals("2")){
			return "1";
		}else{
			return "2";
		}
	}
}
