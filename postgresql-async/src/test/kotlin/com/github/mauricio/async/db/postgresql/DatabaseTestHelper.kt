/*
 * Copyright 2013 Maurício Linhares
 *
 * Maurício Linhares licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.github.mauricio.async.db.postgresql

import com.github.mauricio.async.db.util.Log
import com.github.mauricio.async.db.{Connection, Configuration}
import java.io.File
import java.util.concurrent.{TimeoutException, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{Future, Await}
import com.github.mauricio.async.db.SSLConfiguration
import com.github.mauricio.async.db.SSLConfiguration.Mode

object DatabaseTestHelper {
  val log = Log.get[DatabaseTestHelper]
}

interface DatabaseTestHelper {


  fun databaseName = Some("netty_driver_test")

  fun timeTestDatabase = Some("netty_driver_time_test")

  fun databasePort = 5432

  fun defaultConfiguration = new Configuration(
    port = databasePort,
    username = "postgres",
    database = databaseName)

  fun timeTestConfiguration = new Configuration(
    port = databasePort,
    username = "postgres",
    database = timeTestDatabase)

  fun withHandler[T](fn: (PostgreSQLConnection) -> T): T = {
    withHandler(this.defaultConfiguration, fn)
  }

  fun withTimeHandler[T](fn: (PostgreSQLConnection) -> T): T = {
    withHandler(this.timeTestConfiguration, fn)
  }

  fun withSSLHandler[T](mode: SSLConfiguration.Mode.Value, host: String = "localhost", rootCert: Option[File] = Some(new File("script/server.crt")))(fn: (PostgreSQLConnection) -> T): T = {
    val config = new Configuration(
    host = host,
    port = databasePort,
    username = "postgres",
    database = databaseName,
    ssl = SSLConfiguration(mode = mode, rootCert = rootCert))
    withHandler(config, fn)
  }

  fun withHandler[T](configuration: Configuration, fn: (PostgreSQLConnection) -> T): T = {

    val handler = new PostgreSQLConnection(configuration)

    try {
      Await.result(handler.connect, Duration(5, SECONDS))
      fn(handler)
    } finally {
      handleTimeout(handler, handler.disconnect)
    }

  }

  fun executeDdl(handler: Connection, data: String, count: Int = 0) = {
    val rows = handleTimeout(handler, {
      Await.result(handler.sendQuery(data), Duration(5, SECONDS)).rowsAffected
    })

    if (rows != count) {
      throw new IllegalStateException("We expected %s rows but there were %s".format(count, rows))
    }

    rows
  }

  private fun handleTimeout[R]( handler : Connection, fn : -> R ) = {
    try {
      fn
    } catch {
      case e : TimeoutException -> {
        throw new IllegalStateException("Timeout executing call from handler -> %s".format( handler))
      }
    }
  }

  fun executeQuery(handler: Connection, data: String) = {
    handleTimeout( handler, {
      Await.result(handler.sendQuery(data), Duration(5, SECONDS))
    } )
  }

  fun executePreparedStatement(
                                handler: Connection,
                                statement: String,
                                values: Array[Any] = Array.empty[Any]) = {
    handleTimeout( handler, {
      Await.result(handler.sendPreparedStatement(statement, values), Duration(5, SECONDS))
    } )
  }

  fun await[T](future: Future[T]): T = {
    Await.result(future, Duration(5, TimeUnit.SECONDS))
  }


}
