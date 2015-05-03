import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class BlockMapper extends Mapper<LongWritable,Text,Text,Text>{
	//file format nodeid  blockid pagerank  #outgoingEdges  outgoingEdgeList (delimited by ,)
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String input=value.toString();
		String inputarr[]=input.split("\\s+");
		String nodeID=inputarr[0];
		String blockID = inputarr[1];
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
				context.write(new Text(blockID),new Text(neighborBlockID + " " + blockID + " " + newPR.toString()));
			}
			
		}
		//Remove last comma
		edgeListString = edgeListString.substring(0,edgeListString.length()-1);
		context.write(new Text(blockID), new Text(MainClass.NODEINFO + " " + nodeID + " " + nodePR + " " + edgeListString));

		
	}

	private String blockIDFromNodeID(String edgenode) {
		// TODO Auto-generated method stub
		return null;
	}
}
