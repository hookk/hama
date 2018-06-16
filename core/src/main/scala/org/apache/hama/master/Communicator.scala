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
package org.apache.hama.master

import java.io.IOException
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Callable
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Executors
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path
import org.apache.hama.bsp.Directive
import org.apache.hama.bsp.DirectiveHandler
import org.apache.hama.bsp.GroomServerStatus
import org.apache.hama.bsp.GroomStatusListener
import org.apache.hama.bsp.ReportGroomStatusDirective
import org.apache.hama.conf.Setting
import org.apache.hama.ipc.GroomProtocol
import org.apache.hama.ipc.HamaRPCProtocolVersion
import org.apache.hama.ipc.JobSubmissionProtocol
import org.apache.hama.ipc.MasterProtocol
import org.apache.hama.ipc.RPC
import org.apache.hama.logging.Logging
import org.apache.hama.util.Utils._
import scala.collection.JavaConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

// TODO: move to net package
case class Address(host: String, port: Int) {

  require(!isEmpty(host), s"Host is not provided!")  

  require(isValidPort(port), s"Invalid port range: $port!")  
  
  def inet: InetSocketAddress = new InetSocketAddress(host, port)

}

trait Communicator {
  
  def address(): Address
  
  def start(): Boolean
  
  def stop(): Boolean
  
}

object Communicator {
  
  @deprecated
  class Instructor extends Callable[Boolean] with Logging {
    protected[master] val queue = new LinkedBlockingQueue[Directive]
    protected[master] val handlers = new ConcurrentHashMap[Directive, DirectiveHandler]

    def bind(instruction: Directive, handler: DirectiveHandler) = 
      handlers.putIfAbsent(instruction, handler)

    def put(directive: Directive) = Try(queue.put(directive)) match {
      case Success(_) => true
      case Failure(ex) => false
    }

    @throws(classOf[Exception])
    override def call(): Boolean = {
      while(true) { Try {  
        val directive = queue.take
        handlers.get(directive.getClass).handle(directive)
      } match {
        case Success(_) => 
        case Failure(ex) => ex match {
          case _: InterruptedException => Thread.currentThread().interrupt
          case _: Exception => log.error(s"Failing executing command because $ex")
          
        }
          
      }}
      true
    }
    
  }

  // TODO: 
  def create(host: String, port: Int)(implicit setting: Setting, fs: FileSystem): Communicator = 
    new RPCService(Address(host, port), setting, fs)  
  
}
@deprecated
protected[master] class RPCService(addr: Address, setting: Setting, fs: FileSystem) 
    extends Communicator with MasterProtocol with Logging {
  
  import Communicator._
  
  require(null != setting, "Setting is not provided!")
  require(null != fs, "FileSystem is not provided!")
  
  val executor = Executors.newSingleThreadExecutor
  
  protected[master] val grooms = new ConcurrentHashMap[GroomServerStatus, GroomProtocol]  
  protected[master] val listeners = new CopyOnWriteArrayList[GroomStatusListener]
  protected[master] lazy val rpc = RPC.getServer(
    this, address.host, address.port, setting.hama
  )
  protected[master] lazy val instructor = { 
    val ins = new Instructor      
    /* TODO: 
    instructor.bind(
      classOf[ReportGroomStatusDirective].asInstanceOf[Directive],
      /new ReportGroomStatusHandler()
    )
    */
    ins
  }

  override def address(): Address = addr
    
  def add(listener: GroomStatusListener) = listeners.add(listener)

  def remove(listener: GroomStatusListener) = listeners.remove(listener)

  override def register(status: GroomServerStatus): Boolean = {
    require(null != status, "GroomServerStatus is missing!")
    Try(RPC.waitForProxy(
      classOf[GroomProtocol], 
      HamaRPCProtocolVersion.versionID, 
      address.inet,
      setting.hama
    ).asInstanceOf[GroomProtocol]) match {
      case Success(groom) => 
        grooms.putIfAbsent(status, groom)
        listeners.asScala.foreach(_.groomServerRegistered(status))
        true 
      case Failure(ex) => 
        log.error(s"Fail registering GroomServer because $ex")
        false
    }
  }

  @throws(classOf[IOException])
  override def report(directive: Directive): Boolean = 
    instructor.put(directive)
  
  override def getSystemDir(): String = {
    val sysDir = new Path(setting.hama.get(
      "bsp.system.dir", "/tmp/hadoop/bsp/system"
    ))
    fs.makeQualified(sysDir).toString()
  }
  
  @throws(classOf[IOException])
  override def getProtocolVersion(protocol: String, clientVersion: Long) = 
    if (protocol.equals(classOf[MasterProtocol].getName)) 
      HamaRPCProtocolVersion.versionID
    else if (protocol.equals(classOf[JobSubmissionProtocol].getName)) 
       HamaRPCProtocolVersion.versionID
    else throw new IOException("Unknown protocol to BSPMaster: " + protocol)
  
  override def start(): Boolean = {
    executor.submit(instructor) 
    true
  }
  
  override def stop(): Boolean = {
    executor.shutdownNow
    true
  }

}