
package computation.preprocessing;  

import computation.WccMasterCompute;

import messages.AdjacencyListMessage;
import messages.TriangleCountMessage;
import vertex.WccVertexData;
import utils.ArrayPrimitiveWritable;
import utils.NeighborUtils;
import utils.Debug;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;

import java.util.Set;
import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.utils.MemoryUtils;


public class CountTrianglesComputation extends AbstractComputation<IntWritable,
     WccVertexData, NullWritable, AdjacencyListMessage, TriangleCountMessage> {

  //TODO: Put in superclass
  @Override
  public void postSuperstep() {
    double freeMemory = MemoryUtils.freeMemoryMB()/1000; // Mem in gigs
    double freeNotInHeap = (MemoryUtils.maxMemoryMB() - MemoryUtils.totalMemoryMB())/1000;
    aggregate(WccMasterCompute.MIN_MEMORY_AVAILABLE, new DoubleWritable(freeMemory + freeNotInHeap));
  }

  @Override
  public void compute(
      Vertex<IntWritable, WccVertexData, NullWritable> vertex,
      Iterable<AdjacencyListMessage> messages) {

    updateTriangleCountsAndRespondToSenders(vertex, messages);
  }

  private void updateTriangleCountsAndRespondToSenders(
      Vertex<IntWritable, WccVertexData, NullWritable> vertex,
      Iterable<AdjacencyListMessage> messages) {

    WccVertexData vData = vertex.getValue();
    int t = vData.getT();

    //TODO fig out why i can't use temporary writable here
    for (AdjacencyListMessage m : messages) {
      int messageSource = m.getSourceId();  //TODO refactor into message class
      IntWritable messageSourceWritable = new IntWritable(messageSource);
      int[] neighborNeighbors = m.getAdjacencyList(); 

      //int numCommonNeighbors = NeighborUtils.countCommonNeighbors(vertex, neighborNeighbors);
      int numCommonNeighbors = NeighborUtils.countCommonDuplicateNeighbors(vertex, messageSource, neighborNeighbors); // compatible multi-edges
      if (numCommonNeighbors == 0) {
        // Remove outgoing and incoming edges
        vertex.removeEdges(messageSourceWritable); 
        removeIncomingEdges(vertex.getId(), messageSourceWritable); //TODO refactor into neighborutils
        ///*---display removed edges---------------------------------------------------------
        if (messageSourceWritable.get() > vertex.getId().get()) { // for test
          System.out.println(vertex.getId().get() + "\t" + messageSourceWritable.get());
        } else {
          System.out.println(messageSourceWritable.get() + "\t" + vertex.getId().get());
        }
        //--------------------------------------------------------------------------------*/
      } else {
        t += numCommonNeighbors;
        // Respond to sender with the number of neighbors they have in common
        sendMessage(messageSourceWritable, new TriangleCountMessage(numCommonNeighbors));
      }
    }
    vData.setT(t);
  }

  private void removeIncomingEdges(
      IntWritable destId,
      IntWritable sourceId) {
    try {
      // Remove incoming edge
      removeEdgesRequest(sourceId, destId);
    } catch (IOException e) {
      System.out.println("Problem with removeEdgeRequest");
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
