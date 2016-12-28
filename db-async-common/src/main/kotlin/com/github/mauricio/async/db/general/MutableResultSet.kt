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

package com.github.mauricio.async.db.general

import com.github.mauricio.async.db.RowData
import com.github.mauricio.async.db.ResultSet
import mu.KLogging

class MutableResultSet<T : ColumnData>(
        val columnTypes: List<T>) : ResultSet {

    companion object : KLogging()

    private val rows = mutableListOf<RowData>()
    private val columnMapping: Map<String, Int> = this.columnTypes
            .mapIndexed({ i, t -> Pair(t.name, i) }).toMap()

    override val columnNames: List<String> get() = columnTypes.map { it.name }

    val types: List<Int> get() = this.columnTypes.map { it.dataType }

    val size: Int get() = this.rows.size

    override operator fun get(rowNumber: Int): RowData = rows[rowNumber]!!

    override fun iterator(): Iterator<RowData> = rows.iterator()

    fun addRow(row: Array<Any?>) =
        rows.add(ArrayRowData(this.rows.size, this.columnMapping, row))
}
