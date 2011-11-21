/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.examples;

import java.io.IOException;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.BooleanMessage;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.IntegerMessage;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.util.KeyValuePair;
import org.apache.zookeeper.KeeperException;

public class ShortestPaths extends
    BSP<ShortestPathVertex, ShortestPathVertexArrayWritable, Text, IntWritable> {
  public static final Log LOG = LogFactory.getLog(ShortestPaths.class);

  public static final String START_VERTEX = "shortest.paths.start.vertex.name";
  private final HashMap<String, ShortestPathVertex> vertexLookupMap = new HashMap<String, ShortestPathVertex>();
  private final HashMap<ShortestPathVertex, ShortestPathVertex[]> adjacencyList = new HashMap<ShortestPathVertex, ShortestPathVertex[]>();
  private String masterTask;

  @Override
  public void bsp(
      BSPPeer<ShortestPathVertex, ShortestPathVertexArrayWritable, Text, IntWritable> peer)
      throws IOException, KeeperException, InterruptedException {
    boolean updated = true;
    while (updated) {
      int updatesMade = 0;
      peer.sync();

      IntegerMessage msg = null;
      Deque<ShortestPathVertex> updatedQueue = new LinkedList<ShortestPathVertex>();
      while ((msg = (IntegerMessage) peer.getCurrentMessage()) != null) {
        ShortestPathVertex vertex = vertexLookupMap.get(msg.getTag());
        // check if we need an distance update
        if (vertex.getCost() > msg.getData()) {
          updatesMade++;
          updatedQueue.add(vertex);
          vertex.setCost(msg.getData());
        }
      }
      // synchonize with all grooms if there were updates
      updated = broadcastUpdatesMade(peer, updatesMade);
      // send updates to the adjacents of the updated vertices
      for (ShortestPathVertex vertex : updatedQueue) {
        sendMessageToNeighbors(peer, vertex);
      }
    }
  }

  @Override
  public void setup(
      BSPPeer<ShortestPathVertex, ShortestPathVertexArrayWritable, Text, IntWritable> peer)
      throws IOException, KeeperException, InterruptedException {

    KeyValuePair<ShortestPathVertex, ShortestPathVertexArrayWritable> next = null;
    while ((next = peer.readNext()) != null) {
      adjacencyList.put(next.getKey(), (ShortestPathVertex[]) next.getValue()
          .toArray());
      vertexLookupMap.put(next.getKey().getName(), next.getKey());
    }

    masterTask = peer.getPeerName(0);

    // initial message bypass
    ShortestPathVertex startVertex = vertexLookupMap.get(peer
        .getConfiguration().get(START_VERTEX));

    if (startVertex != null) {
      startVertex.setCost(0);
      sendMessageToNeighbors(peer, startVertex);
    }
  }

  @Override
  public void cleanup(
      BSPPeer<ShortestPathVertex, ShortestPathVertexArrayWritable, Text, IntWritable> peer)
      throws IOException {
    // write our map into hdfs
    for (Entry<ShortestPathVertex, ShortestPathVertex[]> entry : adjacencyList
        .entrySet()) {
      peer.write(new Text(entry.getKey().getName()), new IntWritable(entry
          .getKey().getCost()));
    }
  }

  /**
   * This method broadcasts to a master groom how many updates were made. He
   * simply sums them up and sends a message back to the grooms if sum is
   * greater than zero.
   * 
   * @param peer The peer we got through the BSP method.
   * @param master The assigned master groom name.
   * @param updates How many updates were made?
   * @return True if we need another iteration, False if no updates can be made
   *         anymore.
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  private boolean broadcastUpdatesMade(
      BSPPeer<ShortestPathVertex, ShortestPathVertexArrayWritable, Text, IntWritable> peer,
      int updates) throws IOException, KeeperException, InterruptedException {
    peer.send(masterTask, new IntegerMessage(peer.getPeerName(), updates));
    peer.sync();
    if (peer.getPeerName().equals(masterTask)) {
      int count = 0;
      IntegerMessage message;
      while ((message = (IntegerMessage) peer.getCurrentMessage()) != null) {
        count += message.getData();
      }

      for (String name : peer.getAllPeerNames()) {
        peer.send(name, new BooleanMessage("", count > 0 ? true : false));
      }
    }

    peer.sync();
    BooleanMessage message = (BooleanMessage) peer.getCurrentMessage();
    return message.getData();
  }

  /**
   * This method takes advantage of our partitioning: it uses the vertexID
   * (simply hash of the name) to determine the host where the message belongs
   * to. <br/>
   * It sends the current cost to the adjacent vertex + the edge weight. If cost
   * will be infinity we just going to send infinity, because summing the weight
   * will cause an integer overflow resulting in negative cost.
   * 
   * @param peer The peer we got through the BSP method.
   * @param id The vertex to all adjacent vertices the new cost has to be send.
   * @throws IOException
   */
  private void sendMessageToNeighbors(
      BSPPeer<ShortestPathVertex, ShortestPathVertexArrayWritable, Text, IntWritable> peer,
      ShortestPathVertex id) throws IOException {
    ShortestPathVertex[] outgoingEdges = adjacencyList.get(id);
    for (ShortestPathVertex adjacent : outgoingEdges) {
      int mod = Math.abs((adjacent.hashCode() % peer.getAllPeerNames().length));
      peer.send(peer.getPeerName(mod), new IntegerMessage(adjacent.getName(),
          id.getCost() == Integer.MAX_VALUE ? id.getCost() : id.getCost()
              + adjacent.getWeight()));
    }
  }

  public static void printUsage() {
    System.out.println("Usage: <startNode> <output path> <input path>");
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException, InstantiationException,
      IllegalAccessException {

    if (args.length < 3) {
      printUsage();
      System.exit(-1);
    }

    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();
    BSPJob bsp = new BSPJob(conf, ShortestPaths.class);
    // Set the job name
    bsp.setJobName("Single Source Shortest Path");

    conf.set(START_VERTEX, args[0]);
    bsp.setOutputPath(new Path(args[1]));
    bsp.setInputPath(new Path(args[2]));

    bsp.setBspClass(ShortestPaths.class);
    bsp.setInputFormat(SequenceFileInputFormat.class);
    bsp.setPartitioner(HashPartitioner.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setOutputKeyClass(Text.class);
    bsp.setOutputValueClass(IntWritable.class);

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (double) (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds");
    }
  }

}