package com.example.mobile

import android.content.Context
import android.content.SharedPreferences
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONException
import org.json.JSONObject
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.ArrayDeque

/**
 * Native WebSocket signaling client for CN CALL, backed by OkHttp.
 *
 * Protocol is copied from the current Dart implementation (CallSocket.dart)
 * and the server endpoint server/main.py /ws/{user_id}:
 *
 *   URL      : wss://<host>/ws/<userId>?token=<token>
 *              (host = cn-call11-production.up.railway.app, as ServerConfig.host)
 *   Auth     : the access token is passed as the `token` query parameter.
 *              An invalid/revoked token makes the server send
 *              {"type":"session_invalid","reason":...} and close with 1008.
 *   Handshake: the server sends {"type":"connected","user_id":<id>} right
 *              after accepting; the socket is only usable once that frame
 *              arrives (`connected` becomes true).
 *   Frames   : JSON objects with a string `type` and flat string values
 *              (call_id, target_id, from_id, caller_name, reason,
 *              ring_expires_at, ...).
 *
 * Lifecycle contract:
 *   - Socket closure is NOT call termination. On a close/error the client
 *     schedules an exponential reconnect (1 << clamp(attempt, 0, 5) seconds)
 *     with the same userId/token, unless reconnect was disabled.
 *   - Only {"type":"session_invalid"} disables reconnect and closes the
 *     socket for good.
 *   - This class contains NO call-state logic, NO LiveKit, and touches NO
 *     AudioManager/audio focus/microphone/Telecom/Flutter UI. It is the
 *     transport only.
 */
interface NativeWebSocketListener {
    fun onFrame(type: String, payload: Map<String, String>)
    fun onClosed(code: Int, reason: String)
    fun onError(t: Throwable)
}

/**
 * OkHttp WebSocket client.
 *
 * Not started automatically: nothing connects until `connect(userId, token)`
 * is invoked. Inactive in the current phase unless an external caller drives it.
 */
object NativeWebSocketClient {
    private const val WS_HOST = "cn-call11-production.up.railway.app"
    private const val WS_SCHEME = "wss"
    private const val RECONNECT_DELAY_MAX_SHIFT = 5
    private const val CLOSE_NORMAL = 1000
    private const val CLOSE_SESSION_INVALID = 1008

    // Phase 2: shared signaling-owner marker. Same SharedPreferences file and
    // fully-qualified key that the Flutter side writes ("flutter." prefix is
    // the FlutterSharedPreferences convention).
    private const val PREFERENCES = "FlutterSharedPreferences"
    private const val OWNER_KEY = "flutter.cn_call_ws_owner"

    private val listeners = CopyOnWriteArraySet<NativeWebSocketListener>()

    private val scheduler: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "cn-call-ws-reconnect").apply { isDaemon = true }
        }

    private val reconnectAttempt = AtomicInteger(0)
    private val connecting = AtomicBoolean(false)

    /** Phase 2: set via [configure]; required to read the owner marker. */
    @Volatile private var appContext: Context? = null

    @Volatile private var webSocket: WebSocket? = null
    @Volatile private var ready = false
    @Volatile private var reconnectEnabled = false

    /**
     * Phase 2.1: control frames enqueued while the socket is still mid handshake
     * (a freshly opened socket from a handoff). Flushed on the server
     * "connected" frame and cleared only on terminal teardown so the first
     * frame of a call-operation is delivered exactly once.
     */
    private val pendingFrames = ArrayDeque<String>()
    @Volatile private var reconnectFuture: ScheduledFuture<*>? = null
    @Volatile private var generation = 0
    @Volatile private var currentUserId: String? = null
    @Volatile private var currentToken: String? = null

    /** True only after the server {"type":"connected"} handshake has arrived. */
    @Volatile
    var connected: Boolean = false
        private set

    private val okHttpClient: OkHttpClient by lazy {
        OkHttpClient.Builder()
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(0, TimeUnit.MILLISECONDS)
            .pingInterval(25, TimeUnit.SECONDS)
            .build()
    }

    /** Registers a frame/state listener. */
    fun addListener(listener: NativeWebSocketListener) {
        listeners.add(listener)
    }

    fun removeListener(listener: NativeWebSocketListener) {
        listeners.remove(listener)
    }

    /**
     * Phase 2: records the app context so ownership can be read. Must be
     * called from CNCallEngine.initialize; strictly stores context and never
     * opens a socket.
     */
    fun configure(context: Context) {
        appContext = context.applicationContext
    }

    /** Reads the shared signaling-owner marker ("" when none). */
    fun readOwner(context: Context): String? {
        return context.getSharedPreferences(PREFERENCES, Context.MODE_PRIVATE)
            .getString(OWNER_KEY, "")
    }

    /** Serializes native-side ownership read-check-writes (see [NOTE_CAS]). */
    private val ownershipLock = Any()

    /**
     * True while a Flutter-managed active/ringing call exists. Flutter writes
     * "flutter.cn_call_active_call_id" on any incoming ring/accept
     * (RtcCallManager → CallSession.markCallActive) and clears it on call end
     * (markCallEnded); "flutter.pending_incoming_call" covers a cold-start
     * notification that has not been consumed yet. These block a native
     * handoff so an outgoing call never seizes an in-progress Flutter call.
     */
    private fun flutterHasManagedCall(prefs: SharedPreferences): Boolean {
        val active = prefs.getString("flutter.cn_call_active_call_id", "").orEmpty()
        if (active.isNotEmpty()) return true
        val pending = prefs.getString("flutter.pending_incoming_call", null)
        return !pending.isNullOrEmpty()
    }

    /**
     * Phase 2.1: acquires (or requests a handoff of) native signaling
     * ownership. Returns true when native is (or remains) the owner.
     *
     * Handoff: when Flutter owns the marker but has no managed active/ringing
     * call, native takes ownership ("flutter" → "native"). Flutter observes
     * the loss the moment its socket is closed — the server keeps a single
     * socket per user, so opening native's socket replaces Flutter's — and
     * the Flutter ownership guard then refuses every reconnect. No
     * timestamp/debounce, no explicit control frame.
     *
     * NOTE_CAS: [SharedPreferences] is NOT an atomic compare-and-swap across
     * the Flutter/Android processes; the read-check-write here is only
     * serialized for concurrent native callers via [ownershipLock]. A racing
     * Flutter write can still interleave; CNCallEngine retries the acquisition
     * and the server's single-socket eviction is the final tie-breaker.
     */
    fun tryAcquireNativeOwnership(context: Context): Boolean {
        val prefs = context.getSharedPreferences(PREFERENCES, Context.MODE_PRIVATE)
        synchronized(ownershipLock) {
            val owner = prefs.getString(OWNER_KEY, "")
            when (owner) {
                "native" -> return true
                "flutter" -> {
                    if (flutterHasManagedCall(prefs)) return false
                }
            }
            prefs.edit().putString(OWNER_KEY, "native").apply()
            return true
        }
    }

    /** Phase 2.1: clears native ownership only while native actually holds it. */
    fun releaseNativeOwnership(context: Context) {
        val prefs = context.getSharedPreferences(PREFERENCES, Context.MODE_PRIVATE)
        synchronized(ownershipLock) {
            if (prefs.getString(OWNER_KEY, "") == "native") {
                prefs.edit().remove(OWNER_KEY).apply()
            }
        }
    }

    /**
     * Opens (or re-opens) the authenticated WebSocket for a user.
     *
     * Returns true once the connection attempt has been enqueued (or the
     * socket is already connected with the same credentials). The socket is
     * only usable after [connected] becomes true (server handshake).
     */
    fun connect(userId: String, token: String): Boolean {
        if (userId.isBlank() || token.isBlank()) return false

        // Phase 2.1 ownership gate: a socket may be opened only by the current
        // owner. If native lost (or never acquired) ownership, never open —
        // even during a handoff the socket is opened strictly AFTER ownership
        // is acquired (CNCallEngine.ensureSignalingConnected).
        val context = appContext ?: return false
        if (readOwner(context) != "native") {
            println("[CN CALL][WS] connect refused owner=${readOwner(context)}")
            return false
        }

        var myGeneration = 0
        synchronized(this) {
            if (connected && userId == currentUserId && token == currentToken) return true
            if (connecting.get()) return true

            // Mark connecting + bump the generation atomically BEFORE any
            // stale in-flight attempt can observe itself as current. Once we
            // win here, no other caller can also open a socket, and a loser
            // simply shares this attempt (mirrors CallSocket.connect()).
            connecting.set(true)
            myGeneration = ++generation
            currentUserId = userId
            currentToken = token
            reconnectEnabled = true
            cancelPendingReconnect()
            closeQuietly()
        }

        val url = "$WS_SCHEME://$WS_HOST/ws/$userId?token=$token"
        val request = Request.Builder().url(url).build()

        okHttpClient.newWebSocket(request, object : WebSocketListener() {
            override fun onOpen(ws: WebSocket, response: Response) {
                if (!isCurrentGeneration(myGeneration)) {
                    ws.close(CLOSE_NORMAL, "superseded")
                    return
                }
                // Phase 2: ownership may have been lost while the handshake
                // was in flight; do not keep a socket we no longer own.
                val context = appContext
                if (context != null && readOwner(context) != "native") {
                    ws.close(CLOSE_NORMAL, "ownership-lost")
                    return
                }
                webSocket = ws
                connecting.set(false)
                // Not ready until the server {"type":"connected"} frame arrives.
            }

            override fun onMessage(ws: WebSocket, text: String) {
                if (!isCurrentGeneration(myGeneration)) return
                try {
                    val json = JSONObject(text)
                    val type = json.optString("type", "")
                    val payload = toFlatMap(json)

                    when (type) {
                        "connected" -> {
                            ready = true
                            connected = true
                            reconnectAttempt.set(0)
                            flushPendingFrames()
                            dispatch(type, payload)
                        }
                        "session_invalid" -> {
                            reconnectEnabled = false
                            cancelPendingReconnect()
                            pendingFrames.clear()
                            closeQuietly()
                            dispatch(type, payload)
                        }
                        else -> dispatch(type, payload)
                    }
                } catch (e: JSONException) {
                    notifyError(e)
                }
            }

            override fun onClosing(ws: WebSocket, code: Int, reason: String) {
                ws.close(code, reason)
            }

            override fun onClosed(ws: WebSocket, code: Int, reason: String) {
                if (!isCurrentGeneration(myGeneration)) return
                if (webSocket === ws) webSocket = null
                connecting.set(false)
                ready = false
                connected = false
                notifyClosed(code, reason)
                if (reconnectEnabled && code != CLOSE_SESSION_INVALID) {
                    scheduleReconnect()
                }
            }

            override fun onFailure(ws: WebSocket, t: Throwable, response: Response?) {
                if (!isCurrentGeneration(myGeneration)) return
                if (webSocket === ws) webSocket = null
                connecting.set(false)
                ready = false
                connected = false
                notifyError(t)
                if (reconnectEnabled) scheduleReconnect()
            }
        })
        return true
    }

    /**
     * Sends a JSON control frame {"type": <type>, ...payload}.
     *
     * Phase 2.1: returns true when the frame was handed to OkHttp OR enqueued
     * while a connection attempt is still in flight (a handoff opens the
     * socket inside the very same call-operation that sends the first frame).
     * The queue is flushed when the server {"type":"connected"} handshake
     * arrives and cleared only on terminal teardown, so the first "call" of a
     * handoff is never dropped.
     */
    fun send(type: String, payload: Map<String, String>): Boolean {
        val message = JSONObject()
        message.put("type", type)
        for ((key, value) in payload) message.put(key, value)
        val encoded = message.toString()

        val ws = webSocket
        if (ws != null && ready) return ws.send(encoded)

        // Mid-handshake, either before onOpen (connecting) or between onOpen
        // and the server "connected" frame (socket present but not ready).
        if (connecting.get() || ws != null) {
            synchronized(this) { pendingFrames.addLast(encoded) }
            return true
        }
        return false
    }

    /**
     * Phase 2.3 (E1): drops control frames that were queued while the socket
     * was still mid handshake and never flushed, because their call already
     * ended or was cancelled before the socket reached the server. CNCallEngine
     * calls this on every native-managed call terminal edge so a stale queued
     * "call" of a cancelled call can never be sent later (no ghost ring).
     * Frames already handed to OkHttp are unaffected; safe to call anytime.
     */
    fun clearPendingFrames() {
        synchronized(this) { pendingFrames.clear() }
    }

    /** Disables reconnect, drops any unsent queued frames and closes the socket for good. */
    fun disconnect() {
        generation++
        reconnectEnabled = false
        cancelPendingReconnect()
        connecting.set(false)
        synchronized(this) { pendingFrames.clear() }
        closeQuietly()
        currentUserId = null
        currentToken = null
    }

    /**
     * Reconnect delay in milliseconds, identical to CallSocket.dart:
     *   1 << clamp(attempt, 0, 5)  seconds.
     */
    fun recalcBackoffMs(attempt: Int): Long {
        val shifted = attempt.coerceIn(0, RECONNECT_DELAY_MAX_SHIFT)
        return (1L shl shifted) * 1000L
    }

    // ------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------

    private fun isCurrentGeneration(expected: Int): Boolean = expected == generation

    private fun closeQuietly() {
        val ws = webSocket
        webSocket = null
        ready = false
        connected = false
        try {
            ws?.close(CLOSE_NORMAL, "cn-call-close")
        } catch (_: Exception) {
            // Best effort; OkHttp also cancels on GC/teardown.
        }
    }

    private fun toFlatMap(json: JSONObject): Map<String, String> {
        val result = LinkedHashMap<String, String>(json.length())
        val keys = json.keys()
        while (keys.hasNext()) {
            val key = keys.next()
            val value = json.opt(key)
            result[key] = value?.toString() ?: ""
        }
        return result
    }

    private fun scheduleReconnect() {
        val userId = currentUserId ?: return
        val token = currentToken ?: return
        if (!reconnectEnabled) return
        if (reconnectFuture != null) return

        // Phase 2: reconnect only while native still owns signaling. If
        // ownership was lost, stop permanently (disconnect disables reconnect)
        // so the loser never re-opens a socket and evicts the current owner.
        val context = appContext ?: return
        if (readOwner(context) != "native") {
            println("[CN CALL][WS] reconnect refused owner=${readOwner(context)} -> disconnect")
            disconnect()
            return
        }

        val delayMs = recalcBackoffMs(reconnectAttempt.getAndIncrement())
        reconnectFuture = scheduler.schedule({
            reconnectFuture = null
            try {
                connect(userId, token)
            } catch (e: Exception) {
                scheduleReconnect()
            }
        }, delayMs, TimeUnit.MILLISECONDS)
    }

    private fun cancelPendingReconnect() {
        reconnectFuture?.cancel(false)
        reconnectFuture = null
    }

    private fun flushPendingFrames() {
        val ws = webSocket ?: return
        synchronized(this) {
            while (true) {
                val next = pendingFrames.peekFirst() ?: return
                // Phase 2.3 (E2): only drop a frame after OkHttp accepted it
                // (ws.send == true). On failure leave it at the queue head so
                // the next "connected" (after a reconnect) retries it — a
                // failed send is never silently reported as delivered.
                if (!ws.send(next)) return
                pendingFrames.removeFirst()
            }
        }
    }

    private fun dispatch(type: String, payload: Map<String, String>) {
        for (listener in listeners) {
            try {
                listener.onFrame(type, payload)
            } catch (e: Exception) {
                // A misbehaving listener must not break the socket loop.
            }
        }
    }

    private fun notifyClosed(code: Int, reason: String) {
        for (listener in listeners) {
            try {
                listener.onClosed(code, reason)
            } catch (e: Exception) {
                // Ignored.
            }
        }
    }

    private fun notifyError(t: Throwable) {
        for (listener in listeners) {
            try {
                listener.onError(t)
            } catch (e: Exception) {
                // Ignored.
            }
        }
    }
}
