

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.sun.corba.se.impl.orbutil.graph.Node;


public class IterateBFSReducer extends Reducer<Text, BFSNode, Text, BFSNode> {


	private Text key = new Text();
	private final BFSNode resultNode = new BFSNode();

	
	
	public void reduce(Text nid, Iterable<BFSNode> values,	Context context) throws IOException, InterruptedException {
			
		
//		int minDist = Integer.MAX_INT;
		int minDist = Integer.MAX_VALUE;
		String[] links= null;
//		for(Pair dest: values.getRight()){
		for(BFSNode node: values){
//			if(minDist> dest.getLeft() )
//			minDist = dest.getLeft();
			if(minDist > node.getDistance()){
				minDist = node.getDistance();
			}
//			if(dest.getRight() != null)
//			links = dest.getRight();
			if(node.getDest() != null){
				links = node.getDest();
				context.getCounter(IterateBFS.Counters.ReachableNodesAtReduce).increment(1);
			}
			
		}
		
//		emit(nodeId, new Pair(minDist, links);
		resultNode.set(nid.toString(), links, minDist);
		context.write(nid, resultNode);


	
	}

}