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

package com.github.mauricio.async.db.examples

import com.github.elizarov.async.async
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.util.URLParser
import com.github.mauricio.async.db.util.ExecutorServiceUtils.CachedExecutionContext
import com.github.mauricio.async.db.RowData
import com.github.mauricio.async.db.QueryResult
import com.github.mauricio.async.db.Connection
import io.netty.channel.EventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import java.time.Duration

fun main(args: Array<String>) {

    val configuration = URLParser.parse("jdbc:postgresql://localhost:5432/my_database?username=postgres&password=postgres")
    val connection: Connection = PostgreSQLConnection(configuration)

    val TL: Duration? = null; //Duration.ofSeconds(5)

    async {
        //waitTimeout?
        connection.connect()

        val queryResult = connection.sendQuery("SELECT 0")
        val result = queryResult.rows.let { if (it == null) -1 else it[0][0] }

        //waitTimeout

        println(result)

        connection.disconnect()
    }.get()
}
