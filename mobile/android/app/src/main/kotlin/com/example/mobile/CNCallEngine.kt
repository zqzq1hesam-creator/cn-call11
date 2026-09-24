package com.example.mobile

import android.Manifest
import android.content.Context
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Bundle
import android.telecom.TelecomManager
import androidx.core.content.ContextCompat
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * CN CALL native call-engine boundary.
 *
 * PHASE 1.7: [CNCallEngineImpl] is now a REAL orchestration implementation of
 * [Delegate] built on the three native components:
 *   - NativeWebSocketClient → signaling transport (owns reconnect itself)
 *   - NativeCallTokenHelper  → credentials + LiveKit token (owns HTTP)
 *   - NativeLiveKit          → media transport (owns room lifecycle)
 *
 * Activation is OPT-IN. Nothing connects automatically:
 *   - None of the connect/priming work starts from initialize() (no auto
 *     WebSocket, no auto LiveKit).
 *   - Signaling is only ever written, and LiveKit only ever joined, when a
 *     call-operation is explicitly invoked (startOutgoing/answer/...).
 *   - The FACADE (outer object) is the activation gate for the current phase:
 *     it is real and ready but keeps the old call path (Telecom + Flutter +
 *     FCM) completely untouched — wiring the facade to the live flow is the
 *     next phase.
 *
 * Protocol provenance (never invented here):
 *   - Sending side mirrors mobile/lib/services/rtc_call_manager.dart.
 *   - Receiving/routing rules and terminal states mirror server/main.py
 *     (/ws/{user_id}, message dispatch, active_calls state machine).
 *   Field names used verbatim: call_id, target_id, from_id, caller_name,
 *   reason, ring_expires_at.
 *
 * Frame inventory used by this engine (all verified against the Dart client):
 *   - "call"          (startOutgoing)      caller → server   (rtc_call_manager.dart:425)
 *   - "call_accept"   (answer)             target → server   (:486)
 *   - "call_reject"   (reject)             target → server   (:546)
 *   - "call_cancelled"(disconnect, caller while ringing)      (:556)
 *   - "hangup"        (disconnect)         either side       (:556, :598)
 *   - received: call / call_started / call_accept / call_reject /
 *     call_cancelled / hangup / timeout / signaling_rejected / session_invalid
 *
 * Ownership split (architecture report):
 *   - CNCallEngine → orchestrates signaling + media transport only.
 *   - CNCallConnection → Telecom lifecycle only (state transitions + audio).
 *   - Audio focus/routing/microphone lifecycle → owned by Android Telecom.
 *   The engine never acquires audio focus, never sets MODE_IN_COMMUNICATION,
 *   never routes the speaker for call audio, and never manages the
 *   microphone lifecycle beyond the LiveKit track on/off switch.
 */
object CNCallEngine {

    /**
     * Engine runtime state. Phase 1.7: the engine becomes real and reports
     * COMPONENTS_READY after initialize(), but the facade remains dormant so
     * the current Flutter/Telecom call path is untouched.
     */
    enum class State {
        UNINITIALIZED,
        INITIALIZED,
        COMPONENTS_READY,
        IN_CALL,
        TERMINATED,
    }

    /**
     * Callbacks surfaced up to the Telecom layer (CNCallConnection).
     * These are the single channel that translates media/signaling progress
     * into Connection state.
     */
    interface Callbacks {
        fun onIncomingCallDelivered()
        fun onMediaReady()
        fun onDisconnected()
        fun onRemoteCallStatus(reason: String)
        fun onError(message: String)
    }

    /**
     * The native signaling + media operations implemented by [CNCallEngineImpl].
     */
    interface Delegate {
        fun initialize(context: Context, callbacks: Callbacks): Boolean
        fun startIncoming(callId: String, callerId: String, callerName: String): Boolean

        /**
         * Opens (or reuses) the authenticated native WebSocket for an incoming
         * call while it is still ringing — no frame is sent. Ownership was
         * already reserved by CallFirebaseService before `addNewIncomingCall`,
         * so this lets the CALLEE receive the caller's `call_cancelled` /
         * `timeout` frames and lets `reject()` deliver a real `call_reject` to
         * the caller (which is what closes the caller's ringing UI).
         */
        fun prepareIncomingSignaling(callId: String): Boolean
        fun startOutgoing(callId: String, address: String): Boolean
        fun answer(callId: String): Boolean
        fun reject(callId: String): Boolean
        fun disconnect(callId: String): Boolean
        fun hold(callId: String): Boolean
        fun unhold(callId: String): Boolean
        fun setMute(callId: String, muted: Boolean): Boolean
        fun setSpeaker(callId: String, speaker: Boolean): Boolean
        fun release(callId: String): Boolean
    }

    /**
     * REAL native engine implementation (Phase 1.7), replacing the Phase 1.6
     * placeholder. Behaviour contract mirrors the Dart call path exactly:
     *
     *   - initialize()  primes components only; connects nothing.
     *   - startIncoming() records the call; sends no frame (the invite already
     *     reached us via signaling/FCM — nothing extra is in the protocol).
     *   - startOutgoing() records callId/target and sends the "call" frame.
     *   - answer() validates the current call, sends "call_accept", then AFTER
     *     that signalling step connects LiveKit asynchronously. send() alone is
     *     NOT treated as accepted/connected; media readiness is delivered only
     *     by NativeLiveKit state + the engine listener (onMediaReady).
     *   - reject() sends "call_reject"; LiveKit is torn down only if a media
     *     room is actually present.
     *   - disconnect() sends the correct terminal frame ("call_cancelled" when
     *     a still-ringing caller cancels, otherwise "hangup") and then tears
     *     down the LiveKit room. NativeLiveKit.disconnect() alone NEVER
     *     produces a signaling hangup.
     *   - setMute/setSpeaker forward to NativeLiveKit only (no AudioManager,
     *     no signalling — the protocol has no mute frame).
     *   - hold()/unhold() return false: server/main.py has no hold/unhold
     *     message type (nothing is invented here).
     *   - release() frees the call record only; it never closes the WebSocket
     *     session and sends no frame.
     *
     * Staleness: every asynchronous edge (LiveKit listener events, the async
     * token fetch + connect) re-validates the engine [generation] AND
     * [scoredCallId] captured at the time the operation was launched, before
     * touching state or calling back. Combined with the internal generation
     * guards of NativeWebSocketClient and NativeLiveKit, a stale callback
     * from an older call can never mutate a newer one.
     */
    class CNCallEngineImpl : Delegate {

        // ------------------------------------------------------------
        // Engine state
        // ------------------------------------------------------------

        private val lock = Any()

        @Volatile
        private var callbacks: Callbacks? = null

        @Volatile
        private var appContext: Context? = null

        /** Engine epoch — bumped on every score/clear/end of a call. */
        @Volatile
        private var generation = 0L

        /** The call the engine is currently orchestrating. */
        @Volatile
        private var scoredCallId: String? = null

        /** Most recent incoming-call signal, recorded but not surfaced. */
        @Volatile
        private var pendingIncomingCall: PendingIncomingCall? = null

        /** True once this call has been accepted (answer or received call_accept). */
        @Volatile
        private var acceptedCallId: String? = null

        /** True when this endpoint started the call. */
        @Volatile
        private var isCaller = false

        /** Outgoing target user id (startOutgoing). */
        @Volatile
        private var outgoingTargetId: String? = null

        /** False after {"type":"session_invalid"}: signaling disabled. */
        @Volatile
        private var sessionTokenValid = true

        @Volatile
        private var shutdownPending = false

        /** Last WebSocket transport error, for diagnostics only. */
        @Volatile
        private var lastSignalingError: Throwable? = null

        @Volatile
        private var wsListenerRegistered = false

        @Volatile
        private var liveKitListenerRegistered = false

        /**
         * Phase WS-media: the only call_id for which the media-"connected" frame
         * was already sent. Single-call policy makes one id enough; a new call has
         * a new id, so it never blocks the next call.
         */
        @Volatile
        private var connectedReportedCallId: String? = null

        private data class PendingIncomingCall(
            val callId: String,
            val callerId: String,
            val callerName: String,
        )

        /** Background worker for blocking LiveKit-token fetches. */
        private val worker: ExecutorService by lazy {
            Executors.newSingleThreadExecutor { runnable ->
                Thread(runnable, "cn-call-engine").apply { isDaemon = true }
            }
        }

        // ------------------------------------------------------------
        // Component bridges
        // ------------------------------------------------------------

        private val webSocketListener = object : NativeWebSocketListener {
            override fun onFrame(type: String, payload: Map<String, String>) {
                handleSignalingFrame(type, payload)
            }

            override fun onClosed(code: Int, reason: String) {
                // Transport-only: NativeWebSocketClient owns reconnect, socket
                // closure is NOT call termination.
            }

            override fun onError(t: Throwable) {
                lastSignalingError = t
            }
        }

        private val liveKitListener = object : NativeLiveKitListener {
            override fun onConnected() {
                val current: String? = synchronized(lock) { scoredCallId }
                if (current == null) return

                val t6 = System.currentTimeMillis()
                println("[CN CALL][SPEED_METRICS] T6_livekit_connected call_id=$current ts=$t6")

                val t7 = System.currentTimeMillis()
                println("[CN CALL][SPEED_METRICS] T7_mic_publish_start call_id=$current ts=$t7")

                NativeLiveKit.setMicrophoneEnabled(true) { error ->
                    val stillCurrent = synchronized(lock) {
                        current == scoredCallId
                    }

                    if (!stillCurrent) return@setMicrophoneEnabled

                    if (error != null) {
                        callbacks?.onError(
                            "LiveKit microphone enable failed: ${error.message}",
                        )
                        return@setMicrophoneEnabled
                    }

                    val t8 = System.currentTimeMillis()
                    println("[CN CALL][SPEED_METRICS] T8_mic_published call_id=$current ts=$t8")

                    val reportContext = appContext
                    val reportUserId = reportContext?.let {
                        NativeCallTokenHelper.restoreUserId(it)
                    }
                    val reportTargetId = if (!reportUserId.isNullOrBlank()) {
                        synchronized(lock) {
                            if (current != scoredCallId ||
                                current == connectedReportedCallId
                            ) {
                                null
                            } else {
                                val target =
                                    if (isCaller) {
                                        outgoingTargetId
                                    } else {
                                        pendingIncomingCall?.callerId
                                    }
                                if (target.isNullOrBlank()) {
                                    null
                                } else {
                                    connectedReportedCallId = current
                                    target
                                }
                            }
                        }
                    } else {
                        null
                    }
                    if (reportUserId != null && reportTargetId != null) {
                        val t9 = System.currentTimeMillis()
                        println("[CN CALL][SPEED_METRICS] T9_connected_signaling_sent call_id=$current ts=$t9")
                        val sent = NativeWebSocketClient.send(
                            "connected",
                            mapOf(
                                "call_id" to current,
                                "target_id" to reportTargetId,
                                "from_id" to reportUserId,
                            ),
                        )
                        println(
                            "[CN CALL][ENGINE] media connected frame" +
                                " call_id=$current sent=$sent",
                        )
                    }

                    val t10 = System.currentTimeMillis()
                    println("[CN CALL][SPEED_METRICS] T10_first_usable_audio call_id=$current ts=$t10")

                    println(
                        "[CN CALL][ENGINE] media ready " +
                            "call_id=$current microphone=enabled",
                    )
                    callbacks?.onMediaReady()
                }
            }

            override fun onDisconnected() {
                // A LiveKit room drop is NOT a hangup: never auto-send hangup
                // and never notify onDisconnected from media alone. Call-end
                // decisions belong to signaling; this is recorded as media
                // state only (nothing else is needed in this phase).
            }

            override fun onError(t: Throwable) {
                val current: String? = synchronized(lock) { scoredCallId }
                if (current == null) return
                // Surface the media error only when a call is actually scored;
                // combined with NativeLiveKit's own generation guard a stale
                // room error cannot target an unrelated call.
                callbacks?.onError("LiveKit media error: ${t.message}")
            }
        }

        // ------------------------------------------------------------
        // Delegate implementation
        // ------------------------------------------------------------

        override fun initialize(context: Context, callbacks: Callbacks): Boolean {
            this.appContext = context.applicationContext
            this.callbacks = callbacks
            NativeLiveKit.initialize(context.applicationContext)
            // Phase 2: gives the WS transport the context it needs to read the
            // shared owner marker. Never opens a socket.
            NativeWebSocketClient.configure(context.applicationContext)
            registerWebSocketListenerOnce()
            registerLiveKitListenerOnce()
            return true
        }

        fun bootstrapSignaling(context: Context): Boolean {
            val appCtx = context.applicationContext
            this.appContext = appCtx
            NativeLiveKit.initialize(appCtx)
            NativeWebSocketClient.configure(appCtx)
            registerWebSocketListenerOnce()

            val userId = NativeCallTokenHelper.restoreUserId(appCtx) ?: return false
            val token = NativeCallTokenHelper.restoreAccessToken(appCtx) ?: return false
            if (userId.isBlank() || token.isBlank()) return false

            if (!NativeWebSocketClient.tryAcquireNativeOwnership(appCtx)) {
                println("[CN CALL][ENGINE] bootstrapSignaling refused: failed to acquire native ownership")
                return false
            }

            sessionTokenValid = true
            shutdownPending = false
            val connectedOrConnecting = NativeWebSocketClient.connect(userId.trim(), token.trim())
            println("[CN CALL][ENGINE] bootstrapSignaling completed user_id=$userId ok=$connectedOrConnecting")
            return connectedOrConnecting
        }

        fun shutdownSignaling(context: Context): Boolean {
            val appCtx = context.applicationContext
            if (CNCallRegistry.hasActiveCall()) {
                shutdownPending = true
                println("[CN CALL][ENGINE] shutdownSignaling deferred: active Telecom call exists")
                return false
            }
            shutdownPending = false
            releaseNativeOwnershipIfOwned()
            println("[CN CALL][ENGINE] shutdownSignaling executed")
            return true
        }

        fun notifyTelecomCallEnded(callId: String) {
            clearScoredCall(callId, "telecom_ended")
            if (shutdownPending && !CNCallRegistry.hasActiveCall()) {
                shutdownPending = false
                releaseNativeOwnershipIfOwned()
                println("[CN CALL][ENGINE] deferred shutdownSignaling executed after Telecom call ended call_id=$callId")
            }
        }

        override fun startIncoming(
            callId: String,
            callerId: String,
            callerName: String,
        ): Boolean {
            if (callId.isBlank() || callerId.isBlank()) return false
            synchronized(lock) {
                if (callId == scoredCallId) return true
                // Phase WS-ring: for an Online target the invite already arrived
                // over the native WebSocket (handleSignalingFrame "call" recorded
                // it in pendingIncomingCall) BEFORE Telecom created this
                // Connection. Promote the existing pending call to scored instead
                // of refusing it, so answer()/reject() (which require scoredCallId)
                // keep working exactly as they do on the FCM path.
                val pending = pendingIncomingCall
                if (pending != null && pending.callId == callId) {
                    ++generation
                    scoredCallId = callId
                    isCaller = false
                    outgoingTargetId = null
                    acceptedCallId = null
                    return true
                }
                // Single-call policy at the engine level: refuse a second
                // incoming while the engine already orchestrates another call
                // (ringing or active). Mirrors the outgoing guard below.
                if (scoredCallId != null || pendingIncomingCall != null) {
                    val current = scoredCallId ?: pendingIncomingCall?.callId
                    println(
                        "[CN CALL][ENGINE] incoming refused: another call in engine" +
                            " scored=$current incoming=$callId",
                    )
                    return false
                }
                ++generation
                scoredCallId = callId
                isCaller = false
                outgoingTargetId = null
                acceptedCallId = null
                pendingIncomingCall =
                    PendingIncomingCall(callId, callerId, callerName)
            }
            return true
        }

        override fun startOutgoing(callId: String, address: String): Boolean {
            if (callId.isBlank() || address.isBlank()) return false
            val targetId = parseTargetId(address)
            if (targetId.isBlank()) return false

            val context = appContext ?: return false
            val ownUserId = NativeCallTokenHelper.restoreUserId(context) ?: return false
            if (targetId == ownUserId) {
                println("[CN CALL][ENGINE] outgoing refused self-call call_id=$callId")
                return false
            }

            synchronized(lock) {
                if (callId == scoredCallId && isCaller) return true
                // Single-call policy at the engine level too: refuse a second
                // outgoing while the engine already orchestrates another call.
                if (scoredCallId != null && scoredCallId != callId) {
                    println(
                        "[CN CALL][ENGINE] outgoing refused: another call scored" +
                            " call_id=$callId",
                    )
                    return false
                }
                ++generation
                scoredCallId = callId
                isCaller = true
                outgoingTargetId = targetId
                pendingIncomingCall = null
                acceptedCallId = null
            }

            if (!ensureSignalingConnected()) return false

            val callerName = context
                .getSharedPreferences("FlutterSharedPreferences", Context.MODE_PRIVATE)
                .getString("flutter.cn_call_display_name", null)
                ?.trim()
                ?.takeIf { it.isNotEmpty() }
                ?: "مستخدم CN CALL"

            val sent = NativeWebSocketClient.send(
                "call",
                mapOf(
                    "call_id" to callId,
                    "target_id" to targetId,
                    "from_id" to ownUserId,
                    "caller_name" to callerName,
                ),
            )
            return sent
        }

        override fun prepareIncomingSignaling(callId: String): Boolean {
            val current: Boolean
            val isIncoming: Boolean
            synchronized(lock) {
                current = callId == scoredCallId
                isIncoming = !isCaller && pendingIncomingCall?.callId == callId
            }
            if (!current || !isIncoming) return false
            if (!ensureSignalingConnected()) return false
            println("[CN CALL][ENGINE] incoming signaling ready call_id=$callId")
            return true
        }

        override fun answer(callId: String): Boolean {
            val current: Boolean
            val alreadyAccepted: Boolean
            val targetId: String?
            synchronized(lock) {
                current = callId == scoredCallId
                alreadyAccepted = current && acceptedCallId == callId
                targetId = if (isCaller) outgoingTargetId else pendingIncomingCall?.callerId
            }
            if (current && alreadyAccepted) {
                println("[CN CALL][ENGINE] answer ignored: already accepted call_id=$callId")
                return false
            }
            if (!current) return false
            if (targetId.isNullOrBlank()) return false
            if (!ensureSignalingConnected()) return false

            // Verified: rtc_call_manager.dart _acceptCall() (lines 486-490) and
            // server/main.py "call_accept" (line 1139): target, status ringing.
            // Record that this endpoint has requested acceptance BEFORE the
            // async WebSocket send can race with the server ACK callback.
            synchronized(lock) {
                if (callId == scoredCallId) {
                    acceptedCallId = callId
                }
            }

            val t1 = System.currentTimeMillis()
            println("[CN CALL][SPEED_METRICS] T1_call_accept_sent call_id=$callId ts=$t1")

            val sent = NativeWebSocketClient.send(
                "call_accept",
                mapOf("call_id" to callId, "target_id" to targetId),
            )

            if (!sent) {
                synchronized(lock) {
                    if (callId == scoredCallId) {
                        acceptedCallId = null
                    }
                }
                println(
                    "[CN CALL][ENGINE] call_accept send failed " +
                        "call_id=$callId"
                )
                return false
            }

            println(
                "[CN CALL][ENGINE] call_accept sent; waiting for server ACK " +
                    "call_id=$callId"
            )
            return true
        }

        override fun reject(callId: String): Boolean {
            val current: Boolean
            val targetId: String?
            synchronized(lock) {
                current = callId == scoredCallId
                targetId = if (isCaller) outgoingTargetId else pendingIncomingCall?.callerId
            }
            if (!current) return false
            if (targetId.isNullOrBlank()) return false
            if (!ensureSignalingConnected()) return false

            // Phase 2.3 (E1): a freshly opened or reconnecting socket must not
            // flush frames of a call that is ending now. Cleared BEFORE the
            // terminal frame is queued so the "call_reject" below survives for
            // this same call.
            NativeWebSocketClient.clearPendingFrames()

            // Verified: rtc_call_manager.dart rejectCall() → _cleanupCall()
            // signalType call_reject (lines 543-548) and server/main.py
            // "call_reject" (line 1142): target, status ringing.
            val sent = NativeWebSocketClient.send(
                "call_reject",
                mapOf("call_id" to callId, "target_id" to targetId),
            )

            // Only tear down the media room if one actually exists.
            if (NativeLiveKit.state != NewNativeLiveKitState.IDLE) {
                NativeLiveKit.disconnect()
            }

            synchronized(lock) {
                if (callId == scoredCallId) {
                    ++generation
                    scoredCallId = null
                    pendingIncomingCall = null
                    acceptedCallId = null
                }
            }
            stopCallAudioService(callId)
            // Keep native signaling ownership/session alive so a terminal frame
            // queued during the WebSocket handshake cannot be cleared before
            // it reaches the server.
            return sent
        }

        override fun disconnect(callId: String): Boolean {
            val current: Boolean
            val targetId: String?
            val callerStillRinging: Boolean
            synchronized(lock) {
                current = callId == scoredCallId
                targetId = if (isCaller) outgoingTargetId else pendingIncomingCall?.callerId
                callerStillRinging = isCaller && acceptedCallId != callId
            }
            if (!current) return false
            if (targetId.isNullOrBlank()) return false

            // Verified: rtc_call_manager.dart hangup() (lines 551-559) —
            // caller cancels while still ringing → "call_cancelled",
            // otherwise → "hangup". Both are terminal in server/main.py
            // (lines 1146, 1150).
            ensureSignalingConnected()
            // Phase 2.3 (E1): drop any queued "call" of the outgoing that is
            // being cancelled before the socket connected (ghost-ring fix); the
            // terminal frame below is the only frame that may still go out.
            NativeWebSocketClient.clearPendingFrames()
            val sent = NativeWebSocketClient.send(
                if (callerStillRinging) "call_cancelled" else "hangup",
                mapOf("call_id" to callId, "target_id" to targetId),
            )

            // Media leg end (NativeLiveKit.disconnect is media-only and can
            // never produce a signaling hangup by itself).
            if (NativeLiveKit.state != NewNativeLiveKitState.IDLE) {
                NativeLiveKit.disconnect()
            }

            synchronized(lock) {
                if (callId == scoredCallId) {
                    ++generation
                    scoredCallId = null
                    pendingIncomingCall = null
                    acceptedCallId = null
                    isCaller = false
                    outgoingTargetId = null
                }
            }
            stopCallAudioService(callId)
            // Keep native signaling ownership/session alive so a queued terminal
            // frame can flush after the WebSocket handshake.
            return sent
        }

        override fun hold(callId: String): Boolean = false

        override fun unhold(callId: String): Boolean = false

        override fun setMute(callId: String, muted: Boolean): Boolean {
            if (!isScored(callId)) return false
            // LiveKit track on/off only. No AudioManager and no signaling
            // mute frame (the protocol has no "mute" message type).
            NativeLiveKit.setMicrophoneEnabled(!muted)
            return true
        }

        override fun setSpeaker(callId: String, speaker: Boolean): Boolean {
            if (!isScored(callId)) return false
            // NativeLiveKit.setSpeaker is a documented no-op since Phase 1.5-D
            // (the LiveKit routing API is audioswitch and competes with
            // Telecom). No AudioManager/AudioSwitch routing here.
            NativeLiveKit.setSpeaker(speaker)
            return true
        }

        override fun release(callId: String): Boolean {
            val released: Boolean
            synchronized(lock) {
                released = callId == scoredCallId
                if (released) {
                    ++generation
                    scoredCallId = null
                    pendingIncomingCall = null
                    acceptedCallId = null
                    isCaller = false
                    outgoingTargetId = null
                }
            }
            // Frees call resources only: the WebSocket session is session-
            // scoped and stays connected (no close), and no frame is sent
            // automatically.
            if (released) {
                stopCallAudioService(callId)
                releaseNativeOwnershipIfOwned()
                // Phase 2.3 (E1): call finished; no queued frame may be flushed.
                NativeWebSocketClient.clearPendingFrames()
            }
            return released
        }

        // ------------------------------------------------------------
        // Public lifecycle helpers (also used by the facade hooks)
        // ------------------------------------------------------------

        fun onCallStarted(callId: String) {
            synchronized(lock) {
                ++generation
                scoredCallId = callId
            }
        }

        fun onCallConnected(callId: String) {
            synchronized(lock) {
                if (callId != scoredCallId) return
                // The microphone is enabled by the NativeLiveKit connected
                // callback before Telecom receives onMediaReady().
            }
        }

        fun onCallEnded(reason: String) {
            // Media-only teardown. Never sends hangup: a real signaling
            // hangup travels exclusively through the WebSocket layer
            // (disconnect() with the terminal frame).
            NativeLiveKit.disconnect()
            val endedCallId: String?
            synchronized(lock) {
                endedCallId = scoredCallId
                ++generation
                scoredCallId = null
                pendingIncomingCall = null
                acceptedCallId = null
                isCaller = false
                outgoingTargetId = null
            }
            stopCallAudioService(endedCallId)
            releaseNativeOwnershipIfOwned()
            // Phase 2.3 (E1): hard end; no queued frame may be flushed after
            // the call is over.
            NativeWebSocketClient.clearPendingFrames()
        }

        // ------------------------------------------------------------
        // Internal orchestration
        // ------------------------------------------------------------

        private fun isScored(callId: String): Boolean {
            synchronized(lock) {
                return callId == scoredCallId
            }
        }

        /**
         * Restores session credentials once (NativeCallTokenHelper) and opens/
         * reuses the authenticated WebSocket. Called only from explicit
         * call-operations; NEVER from initialize().
         *
         * Phase 2.1: a native-managed call acquires (or requests a handoff
         * of) signaling ownership FIRST and only then opens the WebSocket.
         * Ownership is seized only when Flutter has no managed active/ringing
         * call, so a Flutter-held idle socket is handed over instead of
         * fighting an eviction war, while a live Flutter call never loses its
         * socket to a native outgoing call.
         */
        private fun ensureSignalingConnected(): Boolean {
            val context = appContext ?: return false
            val userId = NativeCallTokenHelper.restoreUserId(context) ?: return false
            val token = NativeCallTokenHelper.restoreAccessToken(context) ?: return false
            if (userId.isBlank() || token.isBlank()) return false

            // Phase 2.1: native acquires signaling ownership FIRST — requesting
            // a handoff when Flutter owned the socket but has no managed
            // active/ringing call — and only then opens its WebSocket.
            // tryAcquire refuses while a Flutter managed call exists (a
            // genuine conflict), never merely because Flutter was the owner,
            // so an outgoing call does not fail just because Flutter held the
            // socket before it started. If a Flutter connect flips the marker
            // back between acquire and socket-open, native clears its marker
            // (only when it holds it) and reclaims; the attempts are bounded.
            // connect() itself never opens without owner == "native".
            var attempts = 0
            while (true) {
                if (!NativeWebSocketClient.tryAcquireNativeOwnership(context)) {
                    println("[CN CALL][ENGINE] signaling held by a Flutter call; native WS not opened")
                    return false
                }
                if (NativeWebSocketClient.connect(userId.trim(), token.trim())) return true

                // Ownership flipped to a racing Flutter connect before the
                // socket could open. Clear the marker (only if native still
                // holds it, so a live Flutter socket is never left reported
                // as ownerless), then reclaim and retry.
                NativeWebSocketClient.releaseNativeOwnership(context)
                if (++attempts >= 3) {
                    println("[CN CALL][ENGINE] signaling ownership raced 3x; giving up")
                    return false
                }
                println("[CN CALL][ENGINE] signaling ownership raced; retrying acquire")
            }
        }

        /**
         * Phase 2: clears the native ownership marker at terminal edges, but
         * only while native is genuinely the holder, so a Flutter-owned socket
         * (or a no-owner state) is never wrongly removed.
         */
        private fun releaseNativeOwnershipIfOwned() {
            val context = appContext ?: return

            val released =
                NativeWebSocketClient.disconnectAndReleaseNativeOwnership(context)

            if (released) {
                println("[CN CALL][ENGINE] released native WS ownership")
            } else {
                println(
                    "[CN CALL][ENGINE] native WS ownership release skipped",
                )
            }
        }

        /**
         * Tracks the active engine generation that currently owns the FGS launch.
         */
        @Volatile
        private var fgsOwnerGeneration = 0L

        /**
         * Stops the call-media foreground service at a terminal edge, guarded by generation ownership.
         * Both ownership verification and [CNCallAudioService.stopForCall] execution are serialized inside [lock].
         */
        private fun stopCallAudioService(callId: String?, callerGen: Long? = null) {
            val context = appContext ?: return
            val id = callId?.trim().orEmpty()
            if (id.isEmpty()) return

            synchronized(lock) {
                val owner = fgsOwnerGeneration
                if (owner == 0L) return
                if (callerGen != null && owner != callerGen) {
                    println("[CN CALL][ENGINE] stopCallAudioService skipped: fgs owned by gen=$owner, callerGen=$callerGen")
                    return
                }
                fgsOwnerGeneration = 0L
                CNCallAudioService.stopForCall(context, id)
                println("[CN CALL][ENGINE] mic phone-call FGS stopped call_id=$id gen=$owner")
            }
        }

        /**
         * Direct Design C LiveKit join: uses embedded credentials from signaling,
         * completely bypassing the HTTP token fetch round trip.
         */
        private fun startLiveKitConnectDirect(callId: String, url: String, token: String) {
            val myGeneration: Long
            val context: Context
            synchronized(lock) {
                if (callId != scoredCallId) return
                myGeneration = generation
                fgsOwnerGeneration = generation
                context = appContext ?: return

                val t5 = System.currentTimeMillis()
                println("[CN CALL][SPEED_METRICS] T5_livekit_connect_start call_id=$callId ts=$t5")

                val audioServiceStarted = CNCallAudioService.startForCall(context, callId)
                println(
                    "[CN CALL][ENGINE] mic phone-call FGS call_id=$callId" +
                        " started=$audioServiceStarted gen=$myGeneration",
                )
            }

            val stillCurrent: Boolean
            synchronized(lock) {
                stillCurrent = (myGeneration == generation && callId == scoredCallId && sessionTokenValid)
            }

            if (!stillCurrent) {
                println("[CN CALL][ENGINE] startLiveKitConnectDirect aborted: call became stale during FGS launch call_id=$callId gen=$myGeneration")
                stopCallAudioService(callId, myGeneration)
                return
            }

            NativeLiveKit.connect(url, token, callId)
        }

        /**
         * Fallback HTTP LiveKit join used only when signaling credentials are absent.
         */
        private fun startLiveKitConnectFallback(callId: String) {
            val myGeneration: Long
            val context: Context
            synchronized(lock) {
                if (callId != scoredCallId) return
                myGeneration = generation
            }
            context = appContext ?: return
            val userId = NativeCallTokenHelper.restoreUserId(context) ?: return

            println("[CN CALL][ENGINE] Design C credentials missing; using HTTP fallback call_id=$callId")

            worker.execute {
                val fetchStart = System.currentTimeMillis()
                val result = NativeCallTokenHelper.fetchLiveKitToken(
                    context,
                    userId,
                    callId,
                )
                val fetchEnd = System.currentTimeMillis()
                println(
                    "[CN CALL][SPEED_METRICS] HTTP_fallback_fetch call_id=$callId" +
                        " duration_ms=${fetchEnd - fetchStart}",
                )

                val stillCurrent: Boolean
                synchronized(lock) {
                    stillCurrent =
                        myGeneration == generation &&
                            callId == scoredCallId &&
                            sessionTokenValid
                }
                if (!stillCurrent) return@execute
                if (result == null) {
                    callbacks?.onError("LiveKit token fetch failed call_id=$callId")
                    return@execute
                }

                val t5 = System.currentTimeMillis()
                println("[CN CALL][SPEED_METRICS] T5_livekit_connect_start call_id=$callId ts=$t5")

                val audioServiceStarted = CNCallAudioService.startForCall(
                    context,
                    callId,
                )
                println(
                    "[CN CALL][ENGINE] mic phone-call FGS call_id=$callId" +
                        " started=$audioServiceStarted",
                )
                NativeLiveKit.connect(result.url, result.token, callId)
            }
        }

        private fun registerWebSocketListenerOnce() {
            synchronized(lock) {
                if (!wsListenerRegistered) {
                    NativeWebSocketClient.addListener(webSocketListener)
                    wsListenerRegistered = true
                }
            }
        }

        private fun registerLiveKitListenerOnce() {
            synchronized(lock) {
                if (!liveKitListenerRegistered) {
                    NativeLiveKit.setListener(liveKitListener)
                    liveKitListenerRegistered = true
                }
            }
        }

        /**
         * Translates an incoming signaling frame into internal engine state
         * only — no Telecom action, no Flutter UI and no automatic LiveKit
         * connect in Phase 1.7. Frames about a call that is not the current
         * one are ignored as stale.
         */
        private fun handleSignalingFrame(type: String, payload: Map<String, String>) {
            val frameCallId = payload["call_id"].orEmpty()
            when (type) {
                "call" -> {
                    // Incoming invitation, forwarded to the target by the
                    // server (main.py lines 1078-1084). An Online target
                    // receives ONLY this WS "call" — the FCM "incoming_call"
                    // handled by CallFirebaseService is the Offline/cold-start
                    // path and is never sent while the target socket is
                    // connected. This branch is therefore the driving channel
                    // for Online targets: it records the invite
                    // (pendingIncomingCall) and, when the native WS holds the
                    // ownership marker, presents the SAME incoming Telecom call
                    // the FCM path would create, so the Samsung InCall UI rings
                    // (Telecom → onCreateIncomingConnection → beginRinging).
                    // A relayed copy for the SAME call / same caller is
                    // idempotent; a genuinely new call while this engine
                    // already manages one is refused (single-call policy).
                    val callerId =
                        (payload["from_id"] ?: payload["caller_id"]).orEmpty()
                    val callerName =
                        payload["caller_name"]
                            ?.trim()
                            ?.takeIf { it.isNotEmpty() }
                            ?: "مستخدم CN CALL"
                    val context = appContext
                    val ownerIsNative =
                        context != null &&
                            NativeWebSocketClient.readOwner(context) == "native"
                    var acceptedForPresentation = false
                    synchronized(lock) {
                        val managedCall = scoredCallId
                            ?: pendingIncomingCall?.callId
                        if (managedCall != null && managedCall != frameCallId) {
                            println(
                                "[CN CALL][ENGINE] incoming \"call\" refused:" +
                                    " engine manages managed=$managedCall" +
                                    " incoming=$frameCallId",
                            )
                            return@handleSignalingFrame
                        }
                        val pending = pendingIncomingCall
                        val sameAsPending =
                            pending != null &&
                                pending.callId == frameCallId &&
                                pending.callerId == callerId
                        if (!sameAsPending) {
                            ++generation
                            pendingIncomingCall =
                                PendingIncomingCall(frameCallId, callerId, callerName)
                        }
                        // Exactly one presenting side wins the per-call ticket
                        // (this WS path or the FCM path, whichever is first);
                        // the other skips, so no call is ever presented twice.
                        if (ownerIsNative &&
                            CNCallRegistry.claimTelecomPresentation(frameCallId)
                        ) {
                            acceptedForPresentation = true
                        }
                    }
                    when {
                        acceptedForPresentation ->
                            println(
                                "[CN CALL][ENGINE] signaling call call_id=$frameCallId",
                            )
                        ownerIsNative ->
                            println(
                                "[CN CALL][ENGINE] signaling call already presented" +
                                    " call_id=$frameCallId",
                            )
                        else ->
                            println(
                                "[CN CALL][ENGINE] signaling call (Flutter-managed)" +
                                    " call_id=$frameCallId",
                            )
                    }
                    if (acceptedForPresentation) {
                        presentIncomingCallToTelecom(frameCallId, callerId, callerName)
                    }
                }

                "call_started" -> {
                    // Caller-side ack that the server accepted the call.
                    // target_online is the server's presence/delivery result:
                    // true  = target socket accepted the initial call frame;
                    // false = target was offline, so FCM is the fallback.
                    val targetOnline =
                        payload["target_online"]
                            ?.trim()
                            ?.equals("true", ignoreCase = true)
                            ?: false

                    var isStale = false
                    synchronized(lock) {
                        if (frameCallId != scoredCallId || !isCaller) {
                            println(
                                "[CN CALL][ENGINE] signaling call_started stale" +
                                    " call_id=$frameCallId",
                            )
                            isStale = true
                        }
                    }

                    if (!isStale) {
                        println(
                            "[CN CALL][ENGINE] signaling call_started" +
                                " call_id=$frameCallId target_online=$targetOnline",
                        )
                        // call_started only confirms that the server
                        // created the ringing call. It is NOT recipient
                        // delivery, so it must not start ringback.

                    }
                }

                "call_delivered" -> {
                    var isStale = false
                    synchronized(lock) {
                        if (frameCallId != scoredCallId || !isCaller) {
                            println(
                                "[CN CALL][ENGINE] signaling call_delivered stale" +
                                    " call_id=$frameCallId",
                            )
                            isStale = true
                        }
                    }

                    if (!isStale) {
                        println(
                            "[CN CALL][ENGINE] signaling call_delivered" +
                                " call_id=$frameCallId",
                        )
                        callbacks?.onIncomingCallDelivered()
                    }
                }

                "call_accept" -> {
                    val shouldStartMedia: Boolean
                    val embeddedUrl = payload["livekit_url"].orEmpty().trim()
                    val embeddedToken = payload["livekit_token"].orEmpty().trim()

                    synchronized(lock) {
                        val matches =
                            frameCallId.isNotEmpty() &&
                                frameCallId == scoredCallId &&
                                isCaller

                        if (matches) {
                            acceptedCallId = frameCallId
                            shouldStartMedia = true
                            val t4 = System.currentTimeMillis()
                            println(
                                "[CN CALL][SPEED_METRICS] T4_caller_call_accept_received call_id=$frameCallId ts=$t4" +
                                    " has_embedded_credentials=${embeddedUrl.isNotEmpty() && embeddedToken.isNotEmpty()}",
                            )
                        } else {
                            shouldStartMedia = false
                            println(
                                "[CN CALL][ENGINE] signaling call_accept stale call_id=$frameCallId",
                            )
                        }
                    }

                    if (shouldStartMedia) {
                        if (embeddedUrl.isNotEmpty() && embeddedToken.isNotEmpty()) {
                            startLiveKitConnectDirect(frameCallId, embeddedUrl, embeddedToken)
                        } else {
                            startLiveKitConnectFallback(frameCallId)
                        }
                    }
                }

                "call_accept_ack" -> {
                    val shouldStartMedia: Boolean
                    val embeddedUrl = payload["livekit_url"].orEmpty().trim()
                    val embeddedToken = payload["livekit_token"].orEmpty().trim()

                    synchronized(lock) {
                        shouldStartMedia =
                            frameCallId.isNotEmpty() &&
                                frameCallId == scoredCallId &&
                                !isCaller &&
                                acceptedCallId == frameCallId

                        if (shouldStartMedia) {
                            val t4 = System.currentTimeMillis()
                            println(
                                "[CN CALL][SPEED_METRICS] T4_callee_call_accept_ack_received call_id=$frameCallId ts=$t4" +
                                    " has_embedded_credentials=${embeddedUrl.isNotEmpty() && embeddedToken.isNotEmpty()}",
                            )
                        }
                    }

                    if (shouldStartMedia) {
                        if (embeddedUrl.isNotEmpty() && embeddedToken.isNotEmpty()) {
                            startLiveKitConnectDirect(frameCallId, embeddedUrl, embeddedToken)
                        } else {
                            startLiveKitConnectFallback(frameCallId)
                        }
                    } else {
                        println(
                            "[CN CALL][ENGINE] stale call_accept_ack " +
                                "call_id=$frameCallId",
                        )
                    }
                }

                "connected_ack" -> {
                    synchronized(lock) {
                        if (frameCallId == scoredCallId) {
                            println("[CN CALL][ENGINE] connected_ack received call_id=$frameCallId")
                        }
                    }
                }

                "missed_call" -> {
                    val context = appContext ?: return@handleSignalingFrame
                    val callerId = payload["caller_id"]
                        ?.trim()
                        ?.ifEmpty { payload["from_id"]?.trim().orEmpty() }
                        .orEmpty()
                    val callerName = payload["caller_name"]
                        ?.trim()
                        ?.takeIf { it.isNotEmpty() }
                        ?: "مستخدم CN CALL"

                    if (frameCallId.isNotEmpty()) {
                        CNCallNotification.showMissed(
                            context,
                            frameCallId,
                            callerName,
                        )
                        println(
                            "[CN CALL][ENGINE] missed_call delivered " +
                                "call_id=$frameCallId caller=$callerId",
                        )
                    }
                }

                "call_reject", "call_cancelled", "hangup", "timeout", "signaling_rejected" -> {
                    val terminalEventId = payload["event_id"].orEmpty().trim()

                    if (terminalEventId.isNotEmpty()) {
                        acknowledgeTerminalEvent(terminalEventId)
                    }

                    val reason = payload["reason"].orEmpty().trim()
                    val spokenStatus = when {
                        type != "call_reject" -> null
                        reason == "offline" -> "offline"
                        reason == "busy" -> "busy"
                        reason == "user_not_found" -> "user_not_found"
                        else -> null
                    }

                    if (spokenStatus != null) {
                        callbacks?.onRemoteCallStatus(spokenStatus)
                    }

                    clearScoredCall(
                        frameCallId,
                        type,
                        notifyDisconnected = spokenStatus == null,
                    )
                }

                "session_invalid" -> {
                    sessionTokenValid = false
                    // Phase 6: a revoked session can never produce a valid
                    // accept for the scored call. Force-invalidate the current
                    // epoch so an in-flight startLiveKitConnect (already past
                    // the token fetch) aborts before NativeLiveKit.connect.
                    clearScoredCallForSessionInvalid()
                    releaseNativeOwnershipIfOwned()
                    // A revoked session also ends any live media leg and the
                    // Telecom connection must reach setDisconnected().
                    if (NativeLiveKit.state != NewNativeLiveKitState.IDLE) {
                        NativeLiveKit.disconnect()
                    }
                    callbacks?.onDisconnected()
                    println("[CN CALL][ENGINE] signaling session_invalid (reconnect disabled)")
                }

                else -> {
                    // Ignored frames stay transport-level, as in the Dart client.
                }
            }
        }

        /**
         * Phase WS-ring: presents the incoming call to Telecom exactly like the
         * FCM path (CallFirebaseService:104), so the original channel (Samsung
         * InCall UI via onCreateIncomingConnection → beginRinging) rings for an
         * Online target whose invite arrived over the native WebSocket. The
         * caller must already have won the per-call presentation slot via
         * [CNCallRegistry.claimTelecomPresentation]; a submission failure
         * releases that slot so the other presenting side (FCM) may still
         * create the call.
         */
        private fun presentIncomingCallToTelecom(
            callId: String,
            callerId: String,
            callerName: String,
        ) {
            val context = appContext ?: run {
                CNCallRegistry.releaseTelecomPresentation(callId)
                return
            }
            try {
                val telecomManager =
                    context.getSystemService(TelecomManager::class.java)
                if (telecomManager == null) {
                    println(
                        "[CN CALL][ENGINE] WS incoming launch failed" +
                            " call_id=$callId error=Telecom unavailable",
                    )
                    CNCallRegistry.releaseTelecomPresentation(callId)
                    return
                }
                telecomManager.addNewIncomingCall(
                    CNCallPhoneAccount.handle(context),
                    Bundle().apply {
                        putString(CNCallConnectionService.EXTRA_CALL_ID, callId)
                        putString(CNCallConnectionService.EXTRA_CALLER_ID, callerId)
                        putString(CNCallConnectionService.EXTRA_CALLER_NAME, callerName)
                    },
                )
                println(
                    "[CN CALL][ENGINE] Telecom incoming submitted (WS path)" +
                        " call_id=$callId",
                )
                // Delivery is acknowledged by CNCallConnectionService exactly
                // when Telecom enters RINGING; do not ACK here, or the network
                // path would be measured from addNewIncomingCall() instead of
                // the real native ringing boundary.
            } catch (e: Exception) {
                println(
                    "[CN CALL][ENGINE] WS incoming launch failed call_id=$callId" +
                        " error=$e",
                )
                CNCallRegistry.releaseTelecomPresentation(callId)
            }
        }

        /**
         * Clears engine call state when a terminal signaling frame arrives.
         * Mutates only when the frame references the current epoch's call;
         * late frames from older calls are ignored.
         */
        private fun acknowledgeTerminalEvent(eventId: String) {
            val context = appContext ?: return
            val userId = NativeCallTokenHelper.restoreUserId(context)?.trim()

            if (userId.isNullOrEmpty() || eventId.isBlank()) {
                println(
                    "[CN CALL][ENGINE] terminal_ack skipped" +
                        " event_id=$eventId missing_credentials",
                )
                return
            }

            val sent = NativeWebSocketClient.send(
                "terminal_ack",
                mapOf(
                    "event_id" to eventId,
                    "from_id" to userId,
                ),
            )

            println(
                "[CN CALL][ENGINE] terminal_ack" +
                    " sent=$sent event_id=$eventId user=$userId",
            )
        }

        /**
         * Sends a durable terminal-event ACK from the FCM background path.
         *
         * FCM may start this service without a previously configured native
         * signaling client, so this method performs the same cold-start setup
         * used by delivery signaling:
         *   configure -> acquire/check native ownership -> restore credentials
         *   -> connect -> send terminal_ack.
         *
         * The send remains transport-level best effort. If the socket is still
         * connecting, NativeWebSocketClient may queue the ACK and flush it
         * after the server handshake. The server-side durable outbox remains
         * responsible for retrying until an actual ACK arrives.
         */
        /**
         * Best-effort terminal-event ACK from the FCM background path.
         *
         * This path must never acquire native WS ownership or open a new
         * signaling socket just to acknowledge a terminal event. A terminal
         * FCM delivery may arrive when there is no active native Telecom call.
         * In that case the durable server outbox remains pending until a later
         * authenticated WebSocket connection can acknowledge the event.
         *
         * When native already owns signaling, the existing socket can carry the
         * ACK without changing ownership state.
         */
        fun acknowledgeTerminalEventFromFcm(
            context: Context,
            eventId: String,
        ): Boolean {
            val id = eventId.trim()
            if (id.isEmpty()) {
                println(
                    "[CN CALL][ENGINE] FCM terminal_ack skipped: missing event_id",
                )
                return false
            }

            val appCtx = context.applicationContext
            NativeWebSocketClient.configure(appCtx)

            if (NativeWebSocketClient.readOwner(appCtx) != "native") {
                println(
                    "[CN CALL][ENGINE] FCM terminal_ack deferred:" +
                        " native signaling owner is not active" +
                        " event_id=$id",
                )
                return false
            }

            val sent = NativeWebSocketClient.send(
                "terminal_ack",
                mapOf(
                    "event_id" to id,
                ),
            )

            println(
                "[CN CALL][ENGINE] FCM terminal_ack" +
                    " event_id=$id sent=$sent",
            )

            return sent
        }

        private fun clearScoredCall(
            callId: String,
            event: String,
            notifyDisconnected: Boolean = true,
        ) {
            val cleared: Boolean
            synchronized(lock) {
                val matchesScored = callId == scoredCallId
                val matchesPending =
                    callId.isNotEmpty() && callId == pendingIncomingCall?.callId
                cleared = matchesScored || matchesPending
                if (cleared) {
                    ++generation
                    scoredCallId = null
                    pendingIncomingCall = null
                    acceptedCallId = null
                    isCaller = false
                    outgoingTargetId = null
                }
            }
            println(
                "[CN CALL][ENGINE] signaling $event call_id=$callId cleared=$cleared",
            )
            if (cleared) {
                stopCallAudioService(callId)
                releaseNativeOwnershipIfOwned()
                // Phase 2.3 (E1): an inbound terminal frame ended this call;
                // nothing queued for it may be flushed afterwards.
                NativeWebSocketClient.clearPendingFrames()
                // A remote-terminated call has no media leg left: leave the
                // LiveKit room for this callId.
                if (NativeLiveKit.state != NewNativeLiveKitState.IDLE) {
                    NativeLiveKit.disconnect()
                }
                // For spoken availability statuses, Telecom is disconnected
                // by CNCallConnection only after the Arabic announcement ends.
                // Generic terminal events keep the original immediate path.
                if (notifyDisconnected) {
                    callbacks?.onDisconnected()
                }
            }
        }

        /**
         * Phase 6: session_invalid must invalidate ANY in-flight media connect.
         * Unlike [clearScoredCall] it does not require a matching call_id: a
         * revoked session can never yield a valid accept, so the current epoch
         * is force-cleared (bumping generation, the worker's veto gate) even if
         * the scored/pending ids are stale. Same mutation set + pendingFrames
         * clearing as the other terminal edges (Phase 2.3).
         */
        private fun clearScoredCallForSessionInvalid() {
            val endedCallId: String?
            synchronized(lock) {
                if (
                    scoredCallId != null ||
                    pendingIncomingCall != null ||
                    acceptedCallId != null
                ) {
                    endedCallId = scoredCallId
                    ++generation
                    scoredCallId = null
                    pendingIncomingCall = null
                    acceptedCallId = null
                    isCaller = false
                    outgoingTargetId = null
                } else {
                    endedCallId = null
                }
            }
            stopCallAudioService(endedCallId)
            NativeWebSocketClient.clearPendingFrames()
        }

        /** Extracts the target user id from an address like "cncall:<id>" or
         * "tel:<id>". Only those two schemes are valid CN CALL addresses; any
         * other (or missing) scheme is rejected. */
        private fun parseTargetId(address: String): String {
            val trimmed = address.trim()
            if (trimmed.isEmpty()) return ""
            val uri = Uri.parse(trimmed)
            return when (uri.scheme?.lowercase()) {
                "cncall", "tel" -> uri.schemeSpecificPart.orEmpty().trim()
                else -> ""
            }
        }
    }

    /** Real delegate. Phase 1.7: a fully functional native engine. */
    private var delegate: Delegate? = null

    /** Engine lifecycle state. */
    @Volatile
    var state: State = State.UNINITIALIZED
        private set

    @Volatile
    private var callbacks: Callbacks? = null

    /**
     * Install the native engine and record the working Context. The
     * permission check is the unchanged existing policy; prime the three
     * components (fully handled inside [CNCallEngineImpl.initialize]);
     * nothing connects automatically. Returns true once COMPONENTS_READY —
     * the engine is real but NOT yet wired into the live call flow (that is
     * the next phase), so current Telecom/Flutter behavior is untouched.
     */
    fun initialize(context: Context, callbacks: Callbacks): Boolean {
        if (!hasRecordAudioPermission(context)) {
            return unavailable(
                "initialize",
                "",
                callbacks,
                "RECORD_AUDIO permission is not granted",
            )
        }
        this.callbacks = callbacks
        if (delegate == null) {
            delegate = CNCallEngineImpl()
        }
        if (state == State.UNINITIALIZED) {
            state = State.INITIALIZED
        } else if (state == State.TERMINATED) {
            // Re-usable after a full teardown.
            state = State.INITIALIZED
        }
        val ready = delegate?.initialize(context, callbacks) == true
        if (ready) {
            state = State.COMPONENTS_READY
        }
        return ready
    }

    /**
     * Acknowledges that an incoming call was successfully submitted to
     * Android Telecom on the callee side.
     *
     * This is the actual delivery signal used for caller ringback:
     * call_started = server accepted/created the call
     * call_delivered = callee successfully submitted it to Telecom
     */
    /**
     * Starts the native signaling connection early for an FCM-delivered
     * incoming call. No signaling frame is sent here.
     *
     * This is intentionally performed before Telecom presentation so that
     * the later call_delivered ACK does not have to wait for a cold WebSocket
     * handshake.
     */
    fun primeIncomingDeliverySignaling(context: Context): Boolean {
        val appCtx = context.applicationContext
        NativeWebSocketClient.configure(appCtx)

        if (!NativeWebSocketClient.tryAcquireNativeOwnership(appCtx)) {
            println(
                "[CN CALL][ENGINE] delivery signaling prime refused:" +
                    " native ownership unavailable",
            )
            return false
        }

        val userId = NativeCallTokenHelper.restoreUserId(appCtx)
        val token = NativeCallTokenHelper.restoreAccessToken(appCtx)

        if (userId.isNullOrBlank() || token.isNullOrBlank()) {
            println(
                "[CN CALL][ENGINE] delivery signaling prime skipped:" +
                    " missing native credentials",
            )
            return false
        }

        val connectedOrConnecting =
            NativeWebSocketClient.connect(userId.trim(), token.trim())

        println(
            "[CN CALL][ENGINE] delivery signaling primed" +
                " connected_or_connecting=$connectedOrConnecting",
        )

        return connectedOrConnecting
    }

    fun acknowledgeIncomingCallDelivered(
        context: Context,
        callId: String,
        callerId: String,
    ): Boolean {
        val id = callId.trim()
        val caller = callerId.trim()

        if (id.isEmpty() || caller.isEmpty()) {
            println(
                "[CN CALL][ENGINE] delivery_ack skipped: missing call_id/caller_id",
            )
            return false
        }

        val appCtx = context.applicationContext
        NativeWebSocketClient.configure(appCtx)

        if (!NativeWebSocketClient.tryAcquireNativeOwnership(appCtx)) {
            println(
                "[CN CALL][ENGINE] delivery_ack refused: native signaling ownership unavailable" +
                    " call_id=$id",
            )
            return false
        }

        val userId = NativeCallTokenHelper.restoreUserId(appCtx)
        val token = NativeCallTokenHelper.restoreAccessToken(appCtx)

        if (userId.isNullOrBlank() || token.isNullOrBlank()) {
            println(
                "[CN CALL][ENGINE] delivery_ack skipped: missing native credentials" +
                    " call_id=$id",
            )
            return false
        }

        val connectedOrConnecting =
            NativeWebSocketClient.connect(userId.trim(), token.trim())

        val sent = NativeWebSocketClient.send(
            "call_delivered",
            mapOf(
                "call_id" to id,
                "target_id" to caller,
                "from_id" to userId.trim(),
            ),
        )

        println(
            "[CN CALL][ENGINE] delivery_ack" +
                " call_id=$id caller=$caller" +
                " connected_or_connecting=$connectedOrConnecting sent=$sent",
        )

        return sent
    }

    fun hasRecordAudioPermission(context: Context): Boolean {
        return ContextCompat.checkSelfPermission(
            context,
            Manifest.permission.RECORD_AUDIO,
        ) == PackageManager.PERMISSION_GRANTED
    }

    fun isReady(): Boolean = state == State.COMPONENTS_READY && delegate != null

    // ------------------------------------------------------------
    // Activation gate (Phase 1.7)
    //
    // These facade methods are the opt-in switch of the native flow. The real
    // implementation lives in [CNCallEngineImpl] and is fully able to run the
    // verified signaling/media protocol, but this phase must NOT take the live
    // session over yet: the Flutter path keeps driving calls, and wiring the
    // facade through to the engine (FCM → Telecom → engine) is the next
    // phase. Hence each returns false here — identical to today's behavior.
    // ------------------------------------------------------------

    fun startIncoming(
        callId: String,
        callerId: String,
        callerName: String,
    ): Boolean {
        val currentDelegate = delegate
            ?: return unavailable("startIncoming", callId, callbacks)

        if (state == State.UNINITIALIZED || state == State.TERMINATED) {
            return unavailable("startIncoming", callId, callbacks, "Native call engine is not initialized")
        }

        return currentDelegate.startIncoming(callId, callerId, callerName)
    }

    fun prepareIncomingSignaling(callId: String): Boolean {
        val currentDelegate = delegate
            ?: return unavailable("prepareIncomingSignaling", callId, callbacks)

        if (state == State.UNINITIALIZED || state == State.TERMINATED) {
            return unavailable("prepareIncomingSignaling", callId, callbacks, "Native call engine is not initialized")
        }

        return currentDelegate.prepareIncomingSignaling(callId)
    }

    fun startOutgoing(callId: String, address: String): Boolean {
        val currentDelegate = delegate
            ?: return unavailable("startOutgoing", callId, callbacks)

        if (state == State.UNINITIALIZED || state == State.TERMINATED) {
            return unavailable("startOutgoing", callId, callbacks, "Native call engine is not initialized")
        }

        return currentDelegate.startOutgoing(callId, address)
    }

    fun answer(callId: String): Boolean {
        val currentDelegate = delegate
            ?: return unavailable("answer", callId, callbacks)

        if (state == State.UNINITIALIZED || state == State.TERMINATED) {
            return unavailable("answer", callId, callbacks, "Native call engine is not initialized")
        }

        return currentDelegate.answer(callId)
    }

    fun reject(callId: String): Boolean {
        val currentDelegate = delegate
            ?: return unavailable("reject", callId, callbacks)

        if (state == State.UNINITIALIZED || state == State.TERMINATED) {
            return unavailable("reject", callId, callbacks, "Native call engine is not initialized")
        }

        return currentDelegate.reject(callId)
    }

    fun disconnect(callId: String): Boolean {
        val currentDelegate = delegate
            ?: return unavailable("disconnect", callId, callbacks)

        if (state == State.UNINITIALIZED || state == State.TERMINATED) {
            return unavailable("disconnect", callId, callbacks, "Native call engine is not initialized")
        }

        return currentDelegate.disconnect(callId)
    }

    fun setMute(callId: String, muted: Boolean): Boolean {
        val currentDelegate = delegate
            ?: return unavailable("setMute", callId, callbacks)

        if (state == State.UNINITIALIZED || state == State.TERMINATED) {
            return unavailable("setMute", callId, callbacks, "Native call engine is not initialized")
        }

        return currentDelegate.setMute(callId, muted)
    }

    fun setSpeaker(callId: String, speaker: Boolean): Boolean {
        val currentDelegate = delegate
            ?: return unavailable("setSpeaker", callId, callbacks)

        if (state == State.UNINITIALIZED || state == State.TERMINATED) {
            return unavailable("setSpeaker", callId, callbacks, "Native call engine is not initialized")
        }

        return currentDelegate.setSpeaker(callId, speaker)
    }

    fun hold(callId: String): Boolean {
        val currentDelegate = delegate
            ?: return unavailable("hold", callId, callbacks)

        if (state == State.UNINITIALIZED || state == State.TERMINATED) {
            return unavailable("hold", callId, callbacks, "Native call engine is not initialized")
        }

        return currentDelegate.hold(callId)
    }

    fun unhold(callId: String): Boolean {
        val currentDelegate = delegate
            ?: return unavailable("unhold", callId, callbacks)

        if (state == State.UNINITIALIZED || state == State.TERMINATED) {
            return unavailable("unhold", callId, callbacks, "Native call engine is not initialized")
        }

        return currentDelegate.unhold(callId)
    }

    fun release(callId: String): Boolean {
        val currentDelegate = delegate
            ?: return unavailable("release", callId, callbacks)

        return currentDelegate.release(callId)
    }

    fun bootstrapSignaling(context: Context): Boolean {
        if (delegate == null) {
            delegate = CNCallEngineImpl()
        }
        if (state == State.UNINITIALIZED || state == State.TERMINATED) {
            state = State.INITIALIZED
        }
        val ready = (delegate as? CNCallEngineImpl)?.bootstrapSignaling(context) == true
        if (ready && state != State.IN_CALL) {
            state = State.COMPONENTS_READY
        }
        return ready
    }

    fun shutdownSignaling(context: Context): Boolean {
        return (delegate as? CNCallEngineImpl)?.shutdownSignaling(context) ?: false
    }

    fun notifyTelecomCallEnded(callId: String) {
        (delegate as? CNCallEngineImpl)?.notifyTelecomCallEnded(callId)
    }

    fun hasActiveCall(): Boolean {
        return CNCallRegistry.hasActiveCall()
    }

    /**
     * Forwards a durable terminal-event ACK request from the background FCM
     * service through the real native engine implementation.
     */
    fun acknowledgeTerminalEventFromFcm(
        context: Context,
        eventId: String,
    ): Boolean {
        return (delegate as? CNCallEngineImpl)
            ?.acknowledgeTerminalEventFromFcm(context, eventId)
            ?: false
    }

    // ------------------------------------------------------------
    // Lifecycle hooks (routed into the real engine)
    // ------------------------------------------------------------

    /**
     * Scores a new call into the engine (fresh epoch). Does NOT start media or
     * signal by itself.
     */
    fun onCallStarted(callId: String) {
        (delegate as? CNCallEngineImpl)?.onCallStarted(callId)
        if (state == State.INITIALIZED || state == State.COMPONENTS_READY) {
            state = State.IN_CALL
        }
    }

    /**
     * Marks the scored call as media-connected after NativeLiveKit has
     * connected and enabled the local microphone.
     */
    fun onCallConnected(callId: String) {
        (delegate as? CNCallEngineImpl)?.onCallConnected(callId)
    }

    /**
     * Tears down the engine's media transport only. NativeLiveKit.disconnect()
     * leaves + releases the room and never sends a signaling hangup; a real
     * hangup comes exclusively from the WebSocket layer.
     */
    fun onCallEnded(reason: String) {
        (delegate as? CNCallEngineImpl)?.onCallEnded(reason)
        state = State.TERMINATED
    }

    private fun unavailable(
        operation: String,
        callId: String,
        callbacks: Callbacks? = null,
        reason: String? = null,
    ): Boolean {
        val message = reason ?: "Native call engine is not configured: $operation"
        println(
            "[CN CALL][TELECOM] $message call_id=$callId",
        )
        callbacks?.onError(message)
        return false
    }
}
