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
package org.apache.hama.conf

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigValueFactory
import org.apache.hama.HamaConfiguration
import org.apache.hama.logging.Logging
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

object Setting {
  
  def create(name: String = "hama"): Setting = 
    new DefaultSetting(ConfigFactory.load(name))
  
}

trait Setting extends Logging {

  def get[T: ClassTag](key: String): Option[T]

  def set[T](key: String, default: T): Setting
  
  def hama(): HamaConfiguration

}

protected[conf] class DefaultSetting(config: Config) extends Setting {
  
  
  override def get[T: ClassTag](key: String): Option[T] = 
    Try(config.getAnyRef(key)) match {
      case Failure(ex) => None
      case Success(value) => Option(value.asInstanceOf[T])
    }

  override def set[T](key: String, default: T): Setting = new DefaultSetting( 
    config.withValue(key, ConfigValueFactory.fromAnyRef(key))
  )
  
  override def hama(): HamaConfiguration = 
    config.entrySet.asScala.foldLeft(new HamaConfiguration) { case (c, e) =>
      val key = e.getKey
      val value = e.getValue
      c.set(key, value.toString)
      c
    }
  
}