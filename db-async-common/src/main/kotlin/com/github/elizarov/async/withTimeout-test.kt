package com.github.elizarov.async

import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

fun main(args: Array<String>) {
    val service = Executors.newSingleThreadScheduledExecutor()

    fun supplySlow(s: String) = CompletableFuture.supplyAsync<String> {
        Thread.sleep(500L)
        s
    }

    val f = async<String> {
        val a = supplySlow("A").await()
        log("a = $a")
        withTimeout(service, Duration.ofSeconds(1)) {
            val b = supplySlow("B").await()
            log("b = $b")
        }
        try {
            withTimeout(service, Duration.ofMillis(750L)) {
                val c = supplySlow("C").await()
                log("c = $c")
                val d = supplySlow("D").await()
                log("d = $d")
            }
        } catch (ex: CancellationException) {
            log("timed out with $ex")
        }
        val e = supplySlow("E").await()
        log("e = $e")
        "done"
    }
    println("f.get() = ${f.get()}")
}
