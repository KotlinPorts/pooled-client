package com.github.mauricio.async.db.postgresql.encoders

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled

object SSLMessageEncoder {

    fun encode(): ByteBuf = Unpooled.buffer().let {
        it.writeInt(8)
        it.writeShort(1234)
        it.writeShort(5679)
        it
    }

}
