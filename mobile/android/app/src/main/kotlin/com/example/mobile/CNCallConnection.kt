package com.example.mobile

import android.content.Context
import android.media.AudioAttributes
import android.media.MediaPlayer
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import android.net.Uri
import android.os.Build
import android.provider.CallLog
import android.telecom.Connection
import android.telecom.DisconnectCause
import android.telecom.VideoProfile
import livekit.org.webrtc.VideoSink
import java.io.IOException

class CNCallConnection(
    private val appContext: Context,
    val callId: String,
    private val incoming: Boolean,
    private val callerId: String,
    private val callerName: String,
    private val address: Uri,
    private val initialVideoState: Int,
) : Connection() {
    @Volatile
    private var terminal = false
    private var answering = false
    @Volatile
    private var active = false
    private val terminalLock = Any()

    private val videoProvider =
        CNCallVideoProvider(callId, object : CNCallVideoProvider.Listener {
            override fun onSessionModifyRequest(
                fromProfile: VideoProfile,
                toProfile: VideoProfile,
            ) {
                CNCallEngine.requestVideoState(
                    callId,
                    fromProfile.videoState,
                    toProfile.videoState,
                )
            }

            override fun onSessionModifyResponse(responseProfile: VideoProfile) {
                CNCallEngine.handleVideoResponse(
                    callId,
                    responseProfile.videoState,
                )
            }

            override fun onSetCamera(cameraId: String) {
                CNCallEngine.switchVideoCamera(callId, cameraId)
            }

            override fun onPreviewRendererChanged(renderer: VideoSink?) {
                CNCallEngine.setLocalVideoRenderer(callId, renderer)
            }

            override fun onDisplayRendererChanged(renderer: VideoSink?) {
                CNCallEngine.setRemoteVideoRenderer(callId, renderer)
            }

            override fun onRequestCameraCapabilities() {
                CNCallEngine.reportCameraCapabilities(callId)
            }

            override fun onSetDeviceOrientation(rotation: Int) {
                CNCallEngine.setVideoOrientation(callId, rotation)
            }

            override fun onSetZoom(value: Float) {
                CNCallEngine.setVideoZoom(callId, value)
            }

            override fun onRequestCallDataUsage() {
                CNCallEngine.reportVideoDataUsage(callId)
            }

            override fun onSetPauseImage(uri: android.net.Uri?) {
                CNCallEngine.setVideoPauseImage(callId, uri)
            }
        })
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

            // LiveKit now owns a ready EGL context. If Samsung delivered the
            // Telecom video surface earlier, let the provider bind it now.
            videoProvider.onLiveKitReady()

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

            val assetFileName = when (reason) {
                "offline" -> "offline.mp3"
                "busy" -> "busy.mp3"
                "user_not_found" -> "user_not_found.mp3"
                "rejected" -> "rejected.mp3"
                else -> return
            }

            println(
                "[CN CALL][TELECOM] announcing remote status " +
                    "call_id=$callId reason=$reason asset=$assetFileName",
            )

            CNCallStatusSpeaker.speak(
                appContext,
                callId,
                assetFileName,
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
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            val connectivityManager =
                appContext.getSystemService(ConnectivityManager::class.java)
            val activeNetwork = connectivityManager?.activeNetwork
            val networkCapabilities =
                activeNetwork?.let(connectivityManager::getNetworkCapabilities)
            if (
                networkCapabilities?.hasTransport(
                    NetworkCapabilities.TRANSPORT_WIFI,
                ) == true
            ) {
                setConnectionProperties(
                    getConnectionProperties() or PROPERTY_WIFI,
                )
                println(
                    "[CN CALL][TELECOM] connection network=wifi call_id=$callId",
                )
            }
        }
        setConnectionCapabilities(
            getConnectionCapabilities() or
                CAPABILITY_SUPPORTS_VT_LOCAL_BIDIRECTIONAL or
                CAPABILITY_SUPPORTS_VT_REMOTE_BIDIRECTIONAL,
        )
        setVideoProvider(videoProvider)
        setVideoState(
            when (initialVideoState) {
                VideoProfile.STATE_AUDIO_ONLY,
                VideoProfile.STATE_BIDIRECTIONAL,
                VideoProfile.STATE_TX_ENABLED,
                VideoProfile.STATE_RX_ENABLED -> initialVideoState
                else -> VideoProfile.STATE_AUDIO_ONLY
            },
        )
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
        answerIncoming(initialVideoState, "default")
    }

    /**
     * Samsung/Telecom calls this overload when the user chooses to answer
     * directly as a video call. The requested video state must be carried into
     * the native answer path; the no-argument onAnswer() cannot express that
     * choice.
     */
    override fun onAnswer(videoState: Int) {
        answerIncoming(videoState, "video")
    }

    private fun answerIncoming(requestedVideoState: Int, source: String) {
        if (terminal || !incoming || answering || active) return
        if (!CNCallRegistry.claimAnswer(callId)) return
        answering = true
        CNCallNotification.cancel(appContext, callId)

        val normalizedVideoState = when (requestedVideoState) {
            VideoProfile.STATE_AUDIO_ONLY,
            VideoProfile.STATE_BIDIRECTIONAL,
            VideoProfile.STATE_TX_ENABLED,
            VideoProfile.STATE_RX_ENABLED -> requestedVideoState
            else -> VideoProfile.STATE_AUDIO_ONLY
        }

        println(
            "[CN CALL][TELECOM] answer requested " +
                "call_id=$callId source=$source videoState=$normalizedVideoState",
        )
        if (normalizedVideoState != VideoProfile.STATE_AUDIO_ONLY) {
            setVideoState(normalizedVideoState)
        }

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

        val wantsInitialVideo = normalizedVideoState and
            (VideoProfile.STATE_TX_ENABLED or VideoProfile.STATE_RX_ENABLED) != 0
        if (wantsInitialVideo) {
            try {
                val videoFgsStarted = CNCallVideoService.startForCall(appContext, callId)
                println(
                    "[CN CALL][TELECOM] pre-started CNCallVideoService onAnswer " +
                        "call_id=$callId ok=$videoFgsStarted",
                )
            } catch (e: Exception) {
                println(
                    "[CN CALL][TELECOM] pre-start CNCallVideoService exception " +
                        "call_id=$callId err=$e",
                )
            }
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
            !CNCallEngine.startIncoming(
                callId,
                callerId,
                callerName,
                normalizedVideoState,
            )
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
        println(
            "[CN CALL][DIAG][CONNECTION onDisconnect] " +
                "call_id=$callId incoming=$incoming answering=$answering " +
                "active=$active terminal=$terminal",
        )
        synchronized(terminalLock) {
            if (terminal || !CNCallRegistry.claimDisconnect(callId)) return
            terminal = true
        }
        stopOutgoingRingback()
        answering = false
        active = false
        CNCallRegistry.markTerminated(callId)
        val disconnected = CNCallEngine.disconnect(callId)
        println(
            "[CN CALL][DIAG][CONNECTION onDisconnect -> ENGINE disconnect] " +
                "call_id=$callId sent=$disconnected",
        )
        CNCallEngine.release(callId)
        setDisconnected(DisconnectCause(DisconnectCause.LOCAL))
        destroyAndRemove()
    }

    override fun onAbort() {
        println(
            "[CN CALL][DIAG][CONNECTION onAbort] " +
                "call_id=$callId incoming=$incoming answering=$answering " +
                "active=$active terminal=$terminal",
        )
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
        println(
            "[CN CALL][DIAG][CONNECTION fail] " +
                "call_id=$callId incoming=$incoming answering=$answering " +
                "active=$active terminal=$terminal code=$code",
        )
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
        terminateFromRemote(DisconnectCause(code))
    }

    internal fun updateVideoState(videoState: Int) {
        if (terminal) return
        setVideoState(videoState)
    }

    internal fun notifyRemoteVideoRequest(videoState: Int) {
        if (terminal) return
        try {
            videoProvider.receiveSessionModifyRequest(
                VideoProfile(videoState),
            )
        } catch (error: Exception) {
            println(
                "[CN CALL][VIDEO] Telecom request notification failed " +
                    "call_id=" + callId +
                    " error=" + error.message,
            )
        }
    }

    internal fun completeVideoSessionModify(
        requestedVideoState: Int,
        responseVideoState: Int,
        status: Int,
    ) {
        if (terminal) return

        val requestedProfile = VideoProfile(requestedVideoState)
        val responseProfile = VideoProfile(responseVideoState)

        try {
            videoProvider.receiveSessionModifyResponse(
                status,
                requestedProfile,
                responseProfile,
            )
        } catch (error: Exception) {
            println(
                "[CN CALL][VIDEO] Telecom response notification failed " +
                    "call_id=" + callId +
                    " error=" + error.message,
            )
        }

        if (status == Connection.VideoProvider.SESSION_MODIFY_REQUEST_SUCCESS) {
            setVideoState(responseVideoState)
        }
    }

    private fun terminateFromRemote(cause: DisconnectCause) {
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
        setDisconnected(cause)
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
        videoProvider.release()
        destroy()
    }
}
