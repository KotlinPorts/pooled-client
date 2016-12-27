package com.github.elizarov.async

import java.time.Instant

fun log(msg: String) = println("${Instant.now()} [${Thread.currentThread().name}] $msg")