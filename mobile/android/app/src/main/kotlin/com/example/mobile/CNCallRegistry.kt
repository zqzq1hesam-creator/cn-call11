package com.example.mobile

import android.telecom.Connection
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

/**
 * Process-local registry for native Telecom connections.
 */
object CNCallRegistry {
    enum class State {
        RINGING,
        ANSWERING,
        ACTIVE,
        TERMINATED,
    }

    data class Entry(
        val callId: String,
        val connection: Connection,
        val incoming: Boolean,
        val state: AtomicReference<State> = AtomicReference(State.RINGING),
    )

    private val entries = ConcurrentHashMap<String, Entry>()

    fun put(entry: Entry): Boolean {
        return entries.putIfAbsent(entry.callId, entry) == null
    }

    fun get(callId: String): Entry? {
        return entries[callId.trim()]
    }

    fun remove(callId: String): Entry? {
        return entries.remove(callId.trim())
    }

    fun contains(callId: String): Boolean {
        return entries.containsKey(callId.trim())
    }

    /**
     * Phase: Dialer/app outgoing single-call policy. True when any native
     * Telecom connection is still live (ringing/answering/active). A second
     * outgoing Connection for any callId is refused while one exists.
     */
    fun hasActiveCall(): Boolean {
        for (entry in entries.values) {
            if (entry.state.get() != State.TERMINATED) return true
        }
        return false
    }

    fun claimAnswer(callId: String): Boolean =
        get(callId)?.state?.compareAndSet(State.RINGING, State.ANSWERING) == true

    fun markActive(callId: String): Boolean =
        get(callId)?.state?.compareAndSet(State.ANSWERING, State.ACTIVE) == true

    fun markOutgoingActive(callId: String): Boolean =
        get(callId)?.state?.compareAndSet(State.RINGING, State.ACTIVE) == true

    fun claimReject(callId: String): Boolean =
        get(callId)?.state?.compareAndSet(State.RINGING, State.TERMINATED) == true

    fun claimDisconnect(callId: String): Boolean {
        val state = get(callId)?.state ?: return false
        while (true) {
            val current = state.get()
            if (current == State.TERMINATED) return false
            if (state.compareAndSet(current, State.TERMINATED)) return true
        }
    }

    fun markTerminated(callId: String) {
        get(callId)?.state?.set(State.TERMINATED)
    }

    // Phase WS-ring: exactly-once Telecom presentation per call_id across the
    // two presenting sides — the native engine's WebSocket "call" handler
    // (Online targets) and CallFirebaseService (Offline/cold-start FCM).
    // claimTelecomPresentation() is the single atomic ticket for a call_id;
    // the loser skips, so one call_id is never presented to Telecom twice
    // (WS then FCM, or FCM then WS — whichever side is actually given the
    // frame by the server). A slot that fails to submit is released so the
    // other side may still present.
    private val telecomPresented = Collections.newSetFromMap(
        ConcurrentHashMap<String, Boolean>(),
    )

    fun claimTelecomPresentation(callId: String): Boolean {
        val id = callId.trim()
        return id.isNotEmpty() && telecomPresented.add(id)
    }

    fun releaseTelecomPresentation(callId: String) {
        telecomPresented.remove(callId.trim())
    }
}
