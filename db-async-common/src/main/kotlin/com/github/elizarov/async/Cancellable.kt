package com.github.elizarov.async

import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater
import kotlin.coroutines.Continuation
import kotlin.coroutines.ContinuationDispatcher

// --------------- core cancellable interfaces ---------------

public interface Cancellable {
    public val isCancelled: Boolean
    public fun registerCancelHandler(handler: CancelHandler): CancelRegistration
}

public interface CancelHandler {
    fun handleCancel(cancellable: Cancellable)
}

public interface CancelRegistration {
    fun unregisterCancelHandler()
}

typealias CancellationException = CancellationException

// --------------- utility classes to simplify cancellable implementation

public object NoCancelRegistration : CancelRegistration {
    override fun unregisterCancelHandler() {}
}

public open class CancellationScope : Cancellable {
    // keeps a stack of cancel listeners or a special CANCELLED, other values denote completed scope
    @Volatile
    protected var state: Any? = ACTIVE

    protected companion object {
        @JvmStatic
        private val STATE: AtomicReferenceFieldUpdater<CancellationScope, Any?> =
            AtomicReferenceFieldUpdater.newUpdater(CancellationScope::class.java, Any::class.java, "state")

        @JvmStatic
        val ACTIVE: Any = ActiveNode() // ACTIVE is ActiveNode

        @JvmStatic
        val CANCELLED: Any = Any() // CANCELLED is NOT ActiveNode
    }

    protected fun compareAndSetState(expect: Any?, update: Any?): Boolean {
        return STATE.compareAndSet(this, expect, update)
    }

    protected open fun unwrapState(state: Any?): Any? = state
    protected open fun rewrapState(prevState: Any?, newState: Any?): Any? = newState

    private val isActive: Boolean get() = unwrapState(state) is ActiveNode

    public override val isCancelled: Boolean get() = state == CANCELLED

    public override fun registerCancelHandler(handler: CancelHandler): CancelRegistration {
        var node: HandlerNode? = null
        while (true) { // lock-free loop on state
            val cur = state
            if (cur == CANCELLED) {
                handler.handleCancel(this)
                return NoCancelRegistration
            }
            val u = unwrapState(cur) as? ActiveNode ?: return NoCancelRegistration  // not active anymore
            if (node == null) node = HandlerNode(this, handler)
            node.lazySetNext(u)
            if (compareAndSetState(cur, rewrapState(cur, node))) return node
        }
    }

    public open fun cancel() {
        while (true) { // lock-free loop on state
            val cur = state
            val u = unwrapState(cur) as? ActiveNode ?: return // not active anymore
            if (compareAndSetState(cur, CANCELLED)) {
                onCancel(cur, u as? HandlerNode)
                return
            }
        }
    }

    private fun onCancel(prevState: Any?, listeners: HandlerNode?)  {
        var suppressedException: Throwable? = null
        var cur: HandlerNode? = listeners
        while (cur != null) {
            val next = cur.next
            if (next is RemovedNode) {
                cur = next.ref
            } else {
                try {
                    cur.handler.handleCancel(this)
                } catch (ex: Throwable) {
                    if (suppressedException != null) ex.addSuppressed(suppressedException)
                    suppressedException = ex
                }
                cur = next as? HandlerNode
            }
        }
        afterCancel(prevState, suppressedException)
    }

    protected open fun afterCancel(prevState: Any?, suppressedException: Throwable?) {
        if (suppressedException != null) throw suppressedException
    }

    protected open class ActiveNode

    private class RemovedNode(val ref: HandlerNode?) : ActiveNode()

    private class HandlerNode(
            val scope: CancellationScope,
            val handler: CancelHandler
    ) : ActiveNode(), CancelRegistration {
        @Volatile
        var next: ActiveNode? = null
        @Volatile
        var prev: ActiveNode? = null // todo: optimize remove via prev pointers

        companion object {
            @JvmStatic
            val NEXT: AtomicReferenceFieldUpdater<HandlerNode, ActiveNode?> =
                AtomicReferenceFieldUpdater.newUpdater(HandlerNode::class.java, ActiveNode::class.java, "next")
            @JvmStatic
            val PREV: AtomicReferenceFieldUpdater<HandlerNode, ActiveNode?> =
                AtomicReferenceFieldUpdater.newUpdater(HandlerNode::class.java, ActiveNode::class.java, "prev")
        }

        fun lazySetNext(next: ActiveNode?) {
            NEXT.lazySet(this, next)
        }

        override fun unregisterCancelHandler() {
            while (true) { // lock-free loop on next
                val cur = next
                if (cur is RemovedNode) return // already unregistered
                if (!scope.isActive) return // scope is not active anymore -- no need to unregister
                if (NEXT.compareAndSet(this, cur, RemovedNode(cur as? HandlerNode))) return // removed successfully
            }
        }
    }
}


