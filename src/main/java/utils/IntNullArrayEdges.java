package utils;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.edge.ReuseObjectsOutEdges;
import org.apache.giraph.edge.MutableOutEdges;
import org.apache.giraph.edge.StrictRandomAccessOutEdges;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.ReusableEdge;
import org.apache.giraph.utils.EdgeIterables;
import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * copy from apache giraph, change long to int 
 * Implementation of {@link OutEdges} with long ids and null edge
 * values, backed by a dynamic primitive array.
 * Parallel edges are allowed.
 * Note: this implementation is optimized for space usage,
 * but random access and edge removals are expensive.
 */
public class IntNullArrayEdges
  implements ReuseObjectsOutEdges<IntWritable, NullWritable>,
  MutableOutEdges<IntWritable, NullWritable>, Trimmable {
  /** Array of target vertex ids. */
  private IntArrayList neighbors;

  @Override
  public void initialize(Iterable<Edge<IntWritable, NullWritable>> edges) {
    EdgeIterables.initialize(this, edges);
  }

  @Override
  public void initialize(int capacity) {
    neighbors = new IntArrayList(capacity);
  }

  @Override
  public void initialize() {
    neighbors = new IntArrayList();
  }

  @Override
  public void add(Edge<IntWritable, NullWritable> edge) {
    neighbors.add(edge.getTargetVertexId().get());
  }

  /**
   * If the backing array is more than four times as big as the number of
   * elements, halve its size.
   */
  private void trimBack() {
    if (neighbors.elements().length > 4 * neighbors.size()) {
      neighbors.trim(neighbors.elements().length / 2);
    }
  }

  /**
   * Remove edge at position i.
   *
   * @param i Position of edge to be removed
   */
  private void removeAt(int i) {
  // The order of the edges is irrelevant, so we can simply replace
  // the deleted edge with the rightmost element, thus achieving constant
  // time.
  if (i == neighbors.size() - 1) {
    neighbors.popInt();
  } else {
    neighbors.set(i, neighbors.popInt());
  }
  // If needed after the removal, trim the array.
  trimBack();
  }

  @Override
  public void remove(IntWritable targetVertexId) {
  // Thanks to the constant-time implementation of removeAt(int),
  // we can remove all matching edges in linear time.
  for (int i = neighbors.size() - 1; i >= 0; --i) {
    if (neighbors.getInt(i) == targetVertexId.get()) {
    removeAt(i);
    }
  }
  }

  @Override
  public int size() {
  return neighbors.size();
  }

  @Override
  public Iterator<Edge<IntWritable, NullWritable>> iterator() {
  // Returns an iterator that reuses objects.
  // The downcast is fine because all concrete Edge implementations are
  // mutable, but we only expose the mutation functionality when appropriate.
  return (Iterator) mutableIterator();
  }

  @Override
  public Iterator<MutableEdge<IntWritable, NullWritable>> mutableIterator() {
  return new Iterator<MutableEdge<IntWritable, NullWritable>>() {
    /** Current position in the array. */
    private int offset = 0;
    /** Representative edge object. */
    private final MutableEdge<IntWritable, NullWritable> representativeEdge =
      EdgeFactory.createReusable(new IntWritable());

    @Override
    public boolean hasNext() {
    return offset < neighbors.size();
    }

    @Override
    public MutableEdge<IntWritable, NullWritable> next() {
    representativeEdge.getTargetVertexId().set(neighbors.getInt(offset++));
    return representativeEdge;
    }

    @Override
    public void remove() {
    // Since removeAt() might replace the deleted edge with the last edge
    // in the array, we need to decrease the offset so that the latter
    // won't be skipped.
    removeAt(--offset);
    }
  };
  }

  @Override
  public void write(DataOutput out) throws IOException {
  out.writeInt(neighbors.size());
  IntIterator neighborsIt = neighbors.iterator();
  while (neighborsIt.hasNext()) {
    out.writeInt(neighborsIt.nextInt());
  }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
  int numEdges = in.readInt();
  initialize(numEdges);
  for (int i = 0; i < numEdges; ++i) {
    neighbors.add(in.readInt());
  }
  }

  @Override
  public void trim() {
  neighbors.trim();
  }
}
