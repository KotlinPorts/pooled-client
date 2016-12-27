package com.github.elizarov.async

import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationDispatcher
import kotlin.coroutines.CoroutineIntrinsics

inline suspend fun <T> suspendDispatchedCoroutineOrReturn(crossinline block: (Continuation<T>, ContinuationDispatcher?) -> Any?): T =
    CoroutineIntrinsics.suspendCoroutineOrReturn { c: Continuation<T> ->
        @Suppress("INVISIBLE_MEMBER", "INVISIBLE_REFERENCE", "NON_PUBLIC_CALL_FROM_PUBLIC_INLINE")
        val d = (c as? kotlin.jvm.internal.DispatchedContinuation<T>)?.dispatcher
        block(c, d)
    }
