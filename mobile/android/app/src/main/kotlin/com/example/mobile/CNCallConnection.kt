package com.example.mobile

import android.content.Context
import android.media.AudioAttributes
import android.media.MediaPlayer
import android.net.Uri
import android.provider.CallLog
import android.telecom.Connection
import android.telecom.DisconnectCause
import java.io.IOException

class CNCallConnection(
    private val appContext: Context,
    val callId: String,
    private val incoming: Boolean,
    private val callerId: String,
    private val callerName: String,
    private val address: Uri,
) : Connection() {
    @Volatile
    private var terminal = false
    private var answering = false
    @Volatile
    private var active = false
    private val terminalLock = Any()
    private val ringbackLock = Any()
    private var ringbackPlayer: MediaPlayer? = null
    private var ringbackGeneration = 0L
    @Volatile
    private var ringbackPrepared = false
    @Volatile
    private var ringbackStartRequested = false
    internal val engineCallbacks = object : CNCallEngine.Callbacks {
        override fun onIncomingCallDelivered() {
            if (terminal || active) return
            if (!incoming) {
                startOutgoingRingback()
            }
        }

        override fun onMediaReady() {
            if (terminal || active) return

            if (incoming) {
                if (answering && CNCallRegistry.markActive(callId)) {
                    active = true
                    answering = false
                    setActive()
                }
                return
            }

            if (CNCallRegistry.markOutgoingActive(callId)) {
                stopOutgoingRingback(markActive = true)
                setActive()
                // App-originated calls keep the in-app CallScreen; the system
                // Dialer path runs headless (Telecom in-call UI only). This
                // push lets the Flutter CallScreen track the same native call.
                MainActivity.postTelecomEvent("active", mapOf("callId" to callId))
            }
        }

        override fun onDisconnected() {
            if (!terminal) {
                fail(DisconnectCause.REMOTE)
            }
        }

        override fun onRemoteCallStatus(reason: String) {
            if (terminal || incoming) return

            stopOutgoingRingback()
            val phrase = when (reason) {
                "offline" ->
                    "المستخدم غير متصل"
                "busy" ->
                    "المستخدم مشغول"
                "user_not_found" ->
                    "المستخدم غير موجود، تأكد من كتابة الرقم بشكل صحيح"
                else -> return
            }

            println(
                "[CN CALL][TELECOM] announcing remote status " +
                    "call_id=$callId reason=$reason",
            )

            CNCallStatusSpeaker.speak(
                appContext,
                callId,
                phrase,
            ) {
                if (!terminal) {
                    fail(DisconnectCause.REMOTE)
                }
            }
        }

        override fun onError(message: String) {
            if (!terminal) {
                println("[CN CALL][TELECOM] engine error call_id=$callId message=$message")
                fail(DisconnectCause.ERROR)
            }
        }
    }

    init {
        setAudioModeIsVoip(true)
        setAddress(address, CallLog.Calls.PRESENTATION_ALLOWED)
        if (!incoming) {
            prepareOutgoingRingback()
        }
        val displayName = callerName.trim()
            .takeIf { it.isNotEmpty() && it != callerId.trim() }
            ?: "مستخدم CN CALL"
        setCallerDisplayName(displayName, CallLog.Calls.PRESENTATION_ALLOWED)
    }

    fun beginRinging() {
        if (!terminal) {
            setRinging()
        }
    }

    fun beginDialing() {
        if (!terminal) {
            setDialing()
        }
    }

    private fun prepareOutgoingRingback() {
        val generation: Long
        synchronized(ringbackLock) {
            if (terminal || active || ringbackPlayer != null) return
            ringbackGeneration += 1
            generation = ringbackGeneration
        }

        println("[CN CALL][RINGBACK] prepare call_id=$callId")

        val player = MediaPlayer()
        var registered = false
        try {
            player.setAudioAttributes(
                AudioAttributes.Builder()
                    .setUsage(AudioAttributes.USAGE_VOICE_COMMUNICATION)
                    .setContentType(AudioAttributes.CONTENT_TYPE_SPEECH)
                    .build(),
            )
            appContext.assets.openFd(
                "flutter_assets/assets/sounds/ringing.mp3",
            ).use { asset ->
                player.setDataSource(
                    asset.fileDescriptor,
                    asset.startOffset,
                    asset.length,
                )
            }
            player.isLooping = true
            player.setOnErrorListener { _, what, extra ->
                println(
                    "[CN CALL][RINGBACK] failed call_id=$callId " +
                        "what=$what extra=$extra",
                )
                stopOutgoingRingback()
                true
            }
            player.setOnPreparedListener { preparedPlayer ->
                var releasePlayer = false
                var shouldStart = false
                synchronized(ringbackLock) {
                    if (preparedPlayer !== ringbackPlayer ||
                        generation != ringbackGeneration ||
                        terminal ||
                        active
                    ) {
                        if (preparedPlayer === ringbackPlayer) {
                            ringbackPlayer = null
                            ringbackPrepared = false
                            ringbackStartRequested = false
                            ringbackGeneration += 1
                            releasePlayer = true
                        }
                    } else {
                        ringbackPrepared = true
                        shouldStart = ringbackStartRequested
                        if (shouldStart) {
                            ringbackStartRequested = false
                            try {
                                preparedPlayer.start()
                            } catch (error: IllegalStateException) {
                                println(
                                    "[CN CALL][RINGBACK] failed call_id=$callId error=$error",
                                )
                                ringbackPlayer = null
                                ringbackPrepared = false
                                ringbackGeneration += 1
                                releasePlayer = true
                                shouldStart = false
                            }
                        }
                    }
                }
                if (releasePlayer) {
                    releaseOutgoingRingbackPlayer(preparedPlayer)
                }
                if (shouldStart) {
                    println("[CN CALL][RINGBACK] started call_id=$callId source=prewarmed")
                } else if (!releasePlayer) {
                    println("[CN CALL][RINGBACK] prepared call_id=$callId")
                }
            }
            synchronized(ringbackLock) {
                if (generation != ringbackGeneration || terminal || active) {
                    releaseOutgoingRingbackPlayer(player)
                    return
                }
                ringbackPlayer = player
                registered = true
                player.prepareAsync()
            }
        } catch (error: IOException) {
            println("[CN CALL][RINGBACK] failed call_id=$callId error=$error")
            if (registered) {
                removeOutgoingRingbackPlayer(player, generation)
            } else {
                releaseOutgoingRingbackPlayer(player)
            }
        } catch (error: IllegalArgumentException) {
            println("[CN CALL][RINGBACK] failed call_id=$callId error=$error")
            if (registered) {
                removeOutgoingRingbackPlayer(player, generation)
            } else {
                releaseOutgoingRingbackPlayer(player)
            }
        } catch (error: IllegalStateException) {
            println("[CN CALL][RINGBACK] failed call_id=$callId error=$error")
            if (registered) {
                removeOutgoingRingbackPlayer(player, generation)
            } else {
                releaseOutgoingRingbackPlayer(player)
            }
        }
    }

    private fun startOutgoingRingback() {
        synchronized(ringbackLock) {
            if (terminal || active || ringbackStartRequested) return
            ringbackStartRequested = true

            val player = ringbackPlayer
            if (!ringbackPrepared || player == null) {
                println("[CN CALL][RINGBACK] waiting prepared player call_id=$callId")
                return
            }

            try {
                player.start()
                ringbackStartRequested = false
            } catch (error: IllegalStateException) {
                println("[CN CALL][RINGBACK] failed call_id=$callId error=$error")
                ringbackPlayer = null
                ringbackPrepared = false
                ringbackGeneration += 1
                releaseOutgoingRingbackPlayer(player)
                ringbackStartRequested = false
                return
            }
        }
        println("[CN CALL][RINGBACK] started call_id=$callId source=delivery")
    }

    private fun removeOutgoingRingbackPlayer(player: MediaPlayer, generation: Long) {
        val removed: Boolean
        synchronized(ringbackLock) {
            removed = if (ringbackPlayer === player && ringbackGeneration == generation) {
                ringbackPlayer = null
                ringbackGeneration += 1
                true
            } else {
                false
            }
        }
        if (removed) {
            releaseOutgoingRingbackPlayer(player)
        }
    }

    private fun releaseOutgoingRingbackPlayer(player: MediaPlayer) {
        try {
            player.release()
        } catch (_: Exception) {
        }
    }

    private fun stopOutgoingRingback(markActive: Boolean = false) {
        val player: MediaPlayer?
        synchronized(ringbackLock) {
            ringbackGeneration += 1
            player = ringbackPlayer
            ringbackPlayer = null
            ringbackPrepared = false
            ringbackStartRequested = false
            if (markActive) {
                active = true
            }
        }
        if (player == null) return

        println("[CN CALL][RINGBACK] stop call_id=$callId")
        try {
            player.stop()
        } catch (_: Exception) {
        } finally {
            try {
                player.release()
            } catch (_: Exception) {
            }
        }
    }

    override fun onAnswer() {
        if (terminal || !incoming || answering || active) return
        if (!CNCallRegistry.claimAnswer(callId)) return
        answering = true
        CNCallNotification.cancel(appContext, callId)

        val t0 = System.currentTimeMillis()
        println("[CN CALL][SPEED_METRICS] T0_answer_pressed call_id=$callId ts=$t0")

        // Pre-start FGS immediately on Main Thread to get microphone context warm
        // before LiveKit attempts track publication.
        try {
            val fgsStarted = CNCallAudioService.startForCall(appContext, callId)
            println("[CN CALL][TELECOM] pre-started CNCallAudioService onAnswer call_id=$callId ok=$fgsStarted")
        } catch (e: Exception) {
            println("[CN CALL][TELECOM] pre-start CNCallAudioService exception call_id=$callId err=$e")
        }

        // Phase 3: only the native signaling owner may answer. If Flutter still
        // owns the socket (a live/ringing Flutter call, or an app that reclaimed
        // ownership in the meantime), refuse here: call_accept is sent exactly
        // once, from CNCallEngine.answer below, and never through a fallback
        // path. For every native cold-start answer the owner marker was already
        // reserved by CallFirebaseService before addNewIncomingCall.
        val owner = NativeWebSocketClient.readOwner(appContext)
        if (owner != "native") {
            println(
                "[CN CALL][TELECOM] answer refused call_id=$callId owner=${owner ?: "(none)"}",
            )
            fail(DisconnectCause.ERROR)
            return
        }
        if (!CNCallEngine.hasRecordAudioPermission(appContext)) {
            fail(DisconnectCause.ERROR)
            return
        }
        if (!CNCallEngine.initialize(appContext, engineCallbacks) ||
            !CNCallEngine.startIncoming(callId, callerId, callerName)
        ) {
            fail(DisconnectCause.ERROR)
            return
        }
        val answerStarted = CNCallEngine.answer(callId)
        if (!answerStarted) {
            fail(DisconnectCause.ERROR)
        }
    }

    override fun onReject() {
        if (terminal || !CNCallRegistry.claimReject(callId)) return
        if (CNCallEngine.reject(callId)) {
            fail(DisconnectCause.REJECTED)
        } else {
            fail(DisconnectCause.ERROR)
        }
    }

    override fun onDisconnect() {
        synchronized(terminalLock) {
            if (terminal || !CNCallRegistry.claimDisconnect(callId)) return
            terminal = true
        }
        stopOutgoingRingback()
        answering = false
        active = false
        CNCallRegistry.markTerminated(callId)
        CNCallEngine.disconnect(callId)
        CNCallEngine.release(callId)
        setDisconnected(DisconnectCause(DisconnectCause.LOCAL))
        destroyAndRemove()
    }

    override fun onAbort() {
        onDisconnect()
    }

    override fun onHold() {
        if (!terminal && active && CNCallEngine.hold(callId)) {
            setOnHold()
        }
    }

    override fun onUnhold() {
        if (!terminal && active && CNCallEngine.unhold(callId)) {
            setActive()
        }
    }

    override fun onMuteStateChanged(isMuted: Boolean) {
        if (!terminal) {
            CNCallEngine.setMute(callId, isMuted)
            MainActivity.postTelecomEvent("muteChanged", mapOf("callId" to callId, "isMuted" to isMuted))
        }
    }

    override fun onCallEndpointChanged(endpoint: android.telecom.CallEndpoint) {
        if (!terminal) {
            println("[CN CALL][TELECOM] CallEndpoint changed call_id=$callId endpoint=$endpoint")
        }
    }

    override fun onAvailableCallEndpointsChanged(availableEndpoints: List<android.telecom.CallEndpoint>) {
        if (!terminal) {
            println("[CN CALL][TELECOM] AvailableCallEndpoints changed call_id=$callId count=${availableEndpoints.size}")
        }
    }

    fun fail(code: Int) {
        synchronized(terminalLock) {
            if (terminal) return
            terminal = true
        }
        stopOutgoingRingback()
        answering = false
        active = false
        CNCallEngine.disconnect(callId)
        CNCallEngine.release(callId)
        CNCallNotification.cancel(appContext, callId)
        setDisconnected(DisconnectCause(code))
        MainActivity.postTelecomEvent("ended", mapOf("callId" to callId))
        destroyAndRemove()
    }

    fun terminateFromRemote(code: Int) {
        synchronized(terminalLock) {
            if (terminal || !CNCallRegistry.claimDisconnect(callId)) return
            terminal = true
        }
        stopOutgoingRingback()
        answering = false
        active = false
        CNCallRegistry.markTerminated(callId)
        CNCallEngine.release(callId)
        CNCallNotification.cancel(appContext, callId)
        setDisconnected(DisconnectCause(code))
        MainActivity.postTelecomEvent("ended", mapOf("callId" to callId))
        destroyAndRemove()
    }

    override fun onShowIncomingCallUi() {
        if (!terminal && incoming) {
            CNCallNotification.showIncoming(appContext, callId, callerName)
        }
    }

    private fun destroyAndRemove() {
        CNCallRegistry.remove(callId)
        CNCallNotification.cancel(appContext, callId)
        CNCallEngine.notifyTelecomCallEnded(callId)
        destroy()
    }
}
