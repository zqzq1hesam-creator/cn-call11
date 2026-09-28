package com.example.mobile

import android.Manifest
import android.content.Context
import android.content.pm.PackageManager
import androidx.core.content.ContextCompat
import io.livekit.android.LiveKit
import io.livekit.android.LiveKitOverrides
import io.livekit.android.util.LoggingLevel
import io.livekit.android.RoomOptions
import io.livekit.android.AudioOptions
import io.livekit.android.audio.NoAudioHandler
import io.livekit.android.events.RoomEvent
import io.livekit.android.events.collect
import io.livekit.android.room.Room
import io.livekit.android.room.track.LocalVideoTrack
import io.livekit.android.room.track.RemoteVideoTrack
import io.livekit.android.room.track.Track
import io.livekit.android.room.participant.VideoTrackPublishDefaults
import io.livekit.android.room.track.LocalVideoTrackOptions
import io.livekit.android.room.track.VideoPreset169
import livekit.org.webrtc.VideoFrame
import livekit.org.webrtc.VideoSink
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch

/**
 * Native LiveKit media-transport layer for CN CALL.
 *
 * PHASE 1.5-D: ACTIVE transport built on the livekit-android 2.11.0 SDK that
 * is already on the Gradle classpath (mobile/android/app/build.gradle.kts).
 *
 * Scope (per the architecture report):
 *   - LiveKit is the PASSIVE RTP/media transport only. This file contains no
 *     call-state machine, no signalling, no Telecom, no AudioManager calls and
 *     no Flutter UI.
 *   - Android Telecom owns audio focus, routing and the microphone lifecycle.
 *     To keep the SDK out of that domain, livekit-android is created with
 *     `AudioOptions(audioHandler = NoAudioHandler())` so it never requests
 *     audio focus, never sets MODE_IN_COMMUNICATION and never drives
 *     speaker/earpiece routing (the default AudioSwitchHandler does all three).
 *
 * Remote audio is played automatically by the SDK's JavaAudioDeviceModule on
 * article subscription (2.11.0 needs no explicit track.start() on Android).
 *
 * The public API below is preserved from the Phase 1 placeholder. The only
 * addition is [initialize], which must be called once with an application
 * Context before the first [connect] because this singleton has no other way
 * to obtain one for [LiveKit.create].
 */
interface NativeLiveKitListener {
    /** Raised when the room has connected and media transport is live. */
    fun onConnected()

    /** Raised when the room disconnects (remote, network, or local). */
    fun onDisconnected()

    /** Raised on a LiveKit/transport error. */
    fun onError(t: Throwable)

    /** Raised when the local camera track changes. */
    fun onLocalVideoTrackChanged(track: LocalVideoTrack?) {}

    /** Raised when the remote camera track changes. */
    fun onRemoteVideoTrackChanged(track: RemoteVideoTrack?) {}
}

enum class NewNativeLiveKitState {
    IDLE,
    CONNECTING,
    CONNECTED,
    DISCONNECTED,
}

/**
 * Behaviour of this object:
 *   - connect() creates and joins a new Room via the SDK, keeping callId as
 *     pure context (never used for call-state decisions; the room/identity is
 *     derived server-side from the token).
 *   - setMicrophoneEnabled() maps to LocalParticipant.setMicrophoneEnabled,
 *     which publishes/unmutes or mutes the local audio track (LiveKit only).
 *   - setSpeaker() is intentionally a no-op in this phase (see below).
 *   - disconnect() leaves + releases the room; it never sends hangup/reject/
 *     cancel and never touches signalling or Telecom.
 *   - Thread safety: all state transitions are guarded by [lock]; a monotonic
 *     [generation] invalidates callbacks/collectors from a superseded Room so
 *     a stale room can never flip the state of a newer connection.
 */
object NativeLiveKit {
    /** Current nominal transport state. */
    @Volatile
    var state: NewNativeLiveKitState = NewNativeLiveKitState.IDLE
        private set

    private val lock = Any()

    /** Application context supplied through [initialize]. */
    @Volatile
    private var appContext: Context? = null

    /**
     * Monotonic attempt id. Every connect()/disconnect() bumps it; every
     * asynchronous callback and event collector checks it before mutating
     * state, so a callback from an old Room can never resurrect CONNECTED.
     */
    @Volatile
    private var generation = 0L

    /** The Room owned by the current generation, if any. */
    @Volatile
    private var room: Room? = null

    @Volatile
    private var localVideoTrack: LocalVideoTrack? = null

    @Volatile
    private var remoteVideoTrack: RemoteVideoTrack? = null

    @Volatile
    private var localVideoRenderer: VideoSink? = null

    @Volatile
    private var remoteVideoRenderer: VideoSink? = null

    /**
     * Keeps the official LocalVideoTrack.addRenderer()/CaptureDispatchObserver
     * path intact while making frame delivery observable and explicit.
     */
    private class ForwardingVideoSink(
        private val label: String,
        private val delegate: VideoSink,
    ) : VideoSink {
        private val frames = AtomicInteger(0)

        override fun onFrame(frame: VideoFrame) {
            val count = frames.incrementAndGet()
            if (count <= 3 || count % 60 == 0) {
                println(
                    "[CN CALL][VIDEO FRAME] " + label + " count=" + count +
                        " size=" + frame.rotatedWidth + "x" + frame.rotatedHeight +
                        " ts=" + frame.timestampNs,
                )
            }
            delegate.onFrame(frame)
        }
    }

    private var eventCollectJob: Job? = null

    private var listener: NativeLiveKitListener? = null

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    fun setListener(value: NativeLiveKitListener?) {
        listener = value
    }

    /**
     * Supplies the application Context needed to build a LiveKit [Room].
     * Must be called before the first [connect]. Stores only the
     * applicationContext; no automatic connection is started here.
     */
    fun initialize(appContext: Context) {
        // Temporary diagnostic: enable LiveKit + underlying WebRTC logging
        // before the first Room is created. No media/Surface behavior changes.
        LiveKit.loggingLevel = LoggingLevel.VERBOSE
        LiveKit.enableWebRTCLogging = true

        synchronized(lock) {
            this.appContext = appContext.applicationContext
        }
    }

    /**
     * Creates and joins a LiveKit room using [url] and [token].
     *
     * [callId] is context-only metadata; it is never used to drive call state
     * (the server derives room name and grants from the token).
     *
     * This transport does not store or fetch its own token and does not touch
     * SharedPreferences. Returns false only when arguments are blank and no
     * application context was initialized, or when a room is already
     * CONNECTED/CONNECTING (idempotent). Returns true once the async join has
     * been enqueued; the result surfaces via [NewNativeLiveKitState] and the
     * [NativeLiveKitListener].
     */
    fun connect(url: String, token: String, callId: String): Boolean {
        if (url.isBlank() || token.isBlank()) return false
        val context = synchronized(lock) { appContext } ?: return false

        val myGeneration: Long = synchronized(lock) {
            val active = room
            if (active != null &&
                (state == NewNativeLiveKitState.CONNECTED ||
                    state == NewNativeLiveKitState.CONNECTING)
            ) {
                return true
            }
            tearDownLocked()
            ++generation
        }

        state = NewNativeLiveKitState.CONNECTING

        scope.launch {
            val newRoom = try {
                LiveKit.create(
                    context,
                    RoomOptions(
                        adaptiveStream = false,
                        dynacast = false,
                        // Two-party Telecom calls use one video layer. The
                        // previous default Simulcast path could leave the
                        // remote endpoint on the 160x90 layer (observed as
                        // 90x160 after rotation). H540 keeps a clear image
                        // while using far less bandwidth than the H720/30
                        // default and avoids sending extra simulcast layers.
                        videoTrackCaptureDefaults = LocalVideoTrackOptions(
                            position = io.livekit.android.room.track.CameraPosition.FRONT,
                            captureParams = VideoPreset169.H540.capture,
                        ),
                        videoTrackPublishDefaults = VideoTrackPublishDefaults(
                            videoEncoding = VideoPreset169.H540.encoding,
                            simulcast = false,
                        ),
                    ),
                    overrides(),
                )
            } catch (e: Exception) {
                if (myGeneration == generation) {
                    state = NewNativeLiveKitState.DISCONNECTED
                    notifyError(e)
                }
                return@launch
            }

            val assigned = synchronized(lock) {
                if (myGeneration != generation) {
                    false
                } else {
                    room = newRoom
                    collectRoomEvents(myGeneration, room!!)
                    true
                }
            }
            if (!assigned) {
                // Superseded by a newer connect()/disconnect().
                try {
                    newRoom.release()
                } catch (_: Exception) {
                    // Ignored: nothing ever connected.
                }
                return@launch
            }

            try {
                newRoom.connect(url, token)
                if (myGeneration == generation) {
                    state = NewNativeLiveKitState.CONNECTED
                    notifyConnected()
                }
            } catch (e: Exception) {
                if (myGeneration == generation) {
                    synchronized(lock) {
                        room = null
                        eventCollectJob?.cancel()
                        eventCollectJob = null
                    }
                    try {
                        newRoom.disconnect()
                    } catch (_: Exception) {
                        // Ignored: the room never joined.
                    }
                    try {
                        newRoom.release()
                    } catch (_: Exception) {
                        // Ignored.
                    }
                    state = NewNativeLiveKitState.DISCONNECTED
                    notifyError(e)
                }
            }
        }
        return true
    }

    /**
     * Enables/disables the local camera and publishes the camera track.
     *
     * Camera permission is intentionally checked by Android before this call;
     * LiveKit owns camera capture and publication after Telecom requests video.
     */
    fun setCameraEnabled(
        enabled: Boolean,
        onComplete: ((Throwable?) -> Unit)? = null,
    ) {
        val target = room
        val attempt = generation

        if (target == null || state != NewNativeLiveKitState.CONNECTED) {
            onComplete?.invoke(
                IllegalStateException("LiveKit room is not connected"),
            )
            return
        }

        scope.launch {
            if (attempt != generation ||
                state != NewNativeLiveKitState.CONNECTED
            ) {
                onComplete?.invoke(
                    IllegalStateException("LiveKit call became stale"),
                )
                return@launch
            }

            try {
                target.localParticipant.setCameraEnabled(enabled)

                if (attempt != generation ||
                    state != NewNativeLiveKitState.CONNECTED
                ) {
                    onComplete?.invoke(
                        IllegalStateException("LiveKit call became stale"),
                    )
                    return@launch
                }

                val publication =
                    target.localParticipant.getTrackPublication(
                        Track.Source.CAMERA,
                    )
                val track =
                    publication?.track as? LocalVideoTrack
                if (enabled && track == null) {
                    throw IllegalStateException(
                        "LiveKit camera track was not published",
                    )
                }

                updateLocalVideoTrack(if (enabled) track else null)
                onComplete?.invoke(null)
            } catch (t: Throwable) {
                if (t is CancellationException) throw t
                onComplete?.invoke(t)
            }
        }
    }

    /**
     * Returns the EGL context owned by the current LiveKit Room.
     * LiveKit's own Room.initVideoRenderer() uses this same context when
     * initializing video renderers; sharing it keeps camera/decoder textures
     * in the same EGL context as the output renderer.
     */
    fun getVideoEglBaseContext(): livekit.org.webrtc.EglBase.Context? {
        val target = room ?: return null
        return try {
            target.lkObjects.eglBase.eglBaseContext
        } catch (error: Throwable) {
            println(
                "[CN CALL][VIDEO] shared EGL context unavailable " +
                    "error=\${error.message}",
            )
            null
        }
    }
    fun setLocalVideoRenderer(renderer: VideoSink?) {
        val oldTrack: LocalVideoTrack?
        val oldRenderer: VideoSink?
        val attachedRenderer = renderer?.let { ForwardingVideoSink("LOCAL", it) }
        val newTrack: LocalVideoTrack?
        synchronized(lock) {
            oldTrack = localVideoTrack
            oldRenderer = localVideoRenderer
            localVideoRenderer = attachedRenderer
            newTrack = localVideoTrack
        }
        println(
            "[CN CALL][VIDEO RENDERER] local changed " +
                "renderer_present=" + (renderer != null) +
                " track_present=" + (newTrack != null),
        )
        if (oldTrack != null && oldRenderer != null) {
            println("[CN CALL][VIDEO RENDERER] local removeRenderer")
            oldTrack.removeRenderer(oldRenderer)
        }
        if (newTrack != null && attachedRenderer != null) {
            println("[CN CALL][VIDEO RENDERER] local addRenderer")
            newTrack.addRenderer(attachedRenderer)
        }
    }

    fun setRemoteVideoRenderer(renderer: VideoSink?) {
        val oldTrack: RemoteVideoTrack?
        val oldRenderer: VideoSink?
        val attachedRenderer = renderer?.let { ForwardingVideoSink("REMOTE", it) }
        val newTrack: RemoteVideoTrack?
        synchronized(lock) {
            oldTrack = remoteVideoTrack
            oldRenderer = remoteVideoRenderer
            remoteVideoRenderer = attachedRenderer
            newTrack = remoteVideoTrack
        }
        println(
            "[CN CALL][VIDEO RENDERER] remote changed " +
                "renderer_present=" + (renderer != null) +
                " track_present=" + (newTrack != null),
        )
        if (oldTrack != null && oldRenderer != null) {
            oldTrack.removeRenderer(oldRenderer)
        }
        if (newTrack != null && attachedRenderer != null) {
            println("[CN CALL][VIDEO RENDERER] remote addRenderer")
            newTrack.addRenderer(attachedRenderer)
        }
    }

    fun switchCamera(
        cameraId: String,
        onComplete: ((Throwable?) -> Unit)? = null,
    ) {
        val track = localVideoTrack
        if (track == null || cameraId.isBlank()) {
            onComplete?.invoke(
                IllegalStateException("Local camera track is not active"),
            )
            return
        }
        scope.launch {
            try {
                track.setDeviceId(cameraId)
                onComplete?.invoke(null)
            } catch (t: Throwable) {
                if (t is CancellationException) throw t
                onComplete?.invoke(t)
            }
        }
    }

    /**
     * Enables/disables the local microphone using the LiveKit SDK only
     * (LocalParticipant.setMicrophoneEnabled: publishes/unmutes or mutes the
     * local audio track). No AudioManager or permission-request handling here;
     * RECORD_AUDIO is preflight-checked, while permission/lifecycle ownership
     * remains with the Android/Telecom layer. No-op unless a room is CONNECTED.
     */
    fun setMicrophoneEnabled(
        enabled: Boolean,
        onComplete: ((Throwable?) -> Unit)? = null,
    ) {
        val target = room
        val attempt = generation
        val context = synchronized(lock) { appContext }

        if (target == null || state != NewNativeLiveKitState.CONNECTED) {
            onComplete?.invoke(
                IllegalStateException("LiveKit room is not connected"),
            )
            return
        }

        scope.launch {
            if (attempt != generation || state != NewNativeLiveKitState.CONNECTED) {
                onComplete?.invoke(
                    IllegalStateException("LiveKit call became stale"),
                )
                return@launch
            }

            try {
                if (enabled &&
                    (context == null ||
                        ContextCompat.checkSelfPermission(
                            context,
                            Manifest.permission.RECORD_AUDIO,
                        ) != PackageManager.PERMISSION_GRANTED)
                ) {
                    val error = SecurityException(
                        "RECORD_AUDIO permission is not granted",
                    )
                    println("[CN CALL][MIC] permission missing")
                    onComplete?.invoke(error)
                    return@launch
                }

                if (!enabled) {
                    target.localParticipant.setMicrophoneEnabled(false)

                    if (attempt == generation) {
                        println("[CN CALL][MIC] DISABLED")
                        onComplete?.invoke(null)
                    }
                    return@launch
                }

                var lastError: Throwable =
                    IllegalStateException("Microphone publication is missing")

                for (attemptNumber in 1..2) {
                    if (attempt != generation ||
                        state != NewNativeLiveKitState.CONNECTED
                    ) {
                        onComplete?.invoke(
                            IllegalStateException("LiveKit call became stale"),
                        )
                        return@launch
                    }

                    println(
                        "[CN CALL][MIC] ENABLE START attempt=$attemptNumber",
                    )

                    try {
                        target.localParticipant.setMicrophoneEnabled(true)
                    } catch (error: Throwable) {
                        lastError = error
                        println(
                            "[CN CALL][MIC] enable failed " +
                                "attempt=$attemptNumber error=${error.message}",
                        )
                    }

                    if (attempt == generation &&
                        state == NewNativeLiveKitState.CONNECTED
                    ) {
                        val publication =
                            target.localParticipant.getTrackPublication(
                                Track.Source.MICROPHONE,
                            )

                        when {
                            publication == null -> {
                                lastError = IllegalStateException(
                                    "Microphone publication is missing",
                                )
                                println("[CN CALL][MIC] PUBLICATION MISSING")
                            }

                            publication.source != Track.Source.MICROPHONE -> {
                                lastError = IllegalStateException(
                                    "Unexpected microphone publication source: " +
                                        "${publication.source}",
                                )
                                println(
                                    "[CN CALL][MIC] WRONG SOURCE " +
                                        "source=${publication.source}",
                                )
                            }

                            publication.kind != Track.Kind.AUDIO -> {
                                lastError = IllegalStateException(
                                    "Unexpected microphone publication kind: " +
                                        "${publication.kind}",
                                )
                                println(
                                    "[CN CALL][MIC] WRONG KIND " +
                                        "kind=${publication.kind}",
                                )
                            }

                            publication.track == null -> {
                                lastError = IllegalStateException(
                                    "Microphone publication track is missing",
                                )
                                println("[CN CALL][MIC] TRACK MISSING")
                            }

                            publication.muted -> {
                                lastError = IllegalStateException(
                                    "Microphone publication is muted",
                                )
                                println("[CN CALL][MIC] PUBLICATION MUTED")
                            }

                            else -> {
                                println(
                                    "[CN CALL][MIC] LOCAL AUDIO PUBLISHED " +
                                        "source=${publication.source} " +
                                        "kind=${publication.kind}",
                                )
                                onComplete?.invoke(null)
                                return@launch
                            }
                        }
                    }

                    if (attemptNumber == 1) {
                        kotlinx.coroutines.delay(300)
                    }
                }

                println(
                    "[CN CALL][MIC] LOCAL AUDIO PUBLISH FAILED " +
                        "error=${lastError.message}",
                )
                if (attempt == generation) {
                    onComplete?.invoke(lastError)
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                if (attempt == generation) {
                    notifyError(e)
                    onComplete?.invoke(e)
                }
            }
        }
    }

    /**
     * Intentionally disabled in Phase 1.5-D.
     *
     * The only speaker/routing APIs livekit-android 2.11.0 exposes are the
     * audioswitch-driven methods on AudioSwitchHandler
     * (selectDevice(AudioDevice.Speakerphone/Earpiece/...), preferredDeviceList).
     * That handler boots itself up by requesting audio focus and setting
     * MODE_IN_COMMUNICATION — exactly what the architecture reserves for
     * Android Telecom. Since NoAudioHandler is used here, there is no
     * LiveKit-owned speaker API that would not compete with Telecom, so this
     * stays a no-op; speaker/earpiece routing is delegated to Telecom.
     */
    fun setSpeaker(speaker: Boolean) {
        @Suppress("UNUSED_PARAMETER")
        val ignored = speaker
    }

    /**
     * Leaves + releases the LiveKit room only. Never sends hangup/reject/
     * cancel, never changes signalling and never touches Telecom. Idempotent;
     * the listener is told onDisconnected() once when an active room was torn
     * down.
     */
    fun disconnect() {
        val prevState: NewNativeLiveKitState
        val hadRoom: Boolean
        synchronized(lock) {
            prevState = state
            ++generation
            hadRoom = room != null
            tearDownLocked()
        }
        state = NewNativeLiveKitState.DISCONNECTED
        if (hadRoom &&
            prevState != NewNativeLiveKitState.CONNECTING
        ) {
            notifyDisconnected()
        }
    }

    private fun clearVideoTracks() {
        val localTrack: LocalVideoTrack?
        val remoteTrack: RemoteVideoTrack?
        val localRenderer: VideoSink?
        val remoteRenderer: VideoSink?
        synchronized(lock) {
            localTrack = localVideoTrack
            remoteTrack = remoteVideoTrack
            localRenderer = localVideoRenderer
            remoteRenderer = remoteVideoRenderer
            localVideoTrack = null
            remoteVideoTrack = null
        }
        if (localTrack != null && localRenderer != null) {
            localTrack.removeRenderer(localRenderer)
        }
        if (remoteTrack != null && remoteRenderer != null) {
            remoteTrack.removeRenderer(remoteRenderer)
        }
    }

    private fun updateLocalVideoTrack(track: LocalVideoTrack?) {
        val oldTrack: LocalVideoTrack?
        val renderer: VideoSink?
        synchronized(lock) {
            oldTrack = localVideoTrack
            localVideoTrack = track
            renderer = localVideoRenderer
        }
        println(
            "[CN CALL][VIDEO TRACK] local changed " +
                "track_present=" + (track != null) +
                " renderer_present=" + (renderer != null),
        )
        if (oldTrack != null && renderer != null) {
            println("[CN CALL][VIDEO TRACK] local removeRenderer")
            oldTrack.removeRenderer(renderer)
        }
        if (track != null && renderer != null) {
            println("[CN CALL][VIDEO TRACK] local addRenderer")
            track.addRenderer(renderer)
        }
        listener?.onLocalVideoTrackChanged(track)
    }

    private fun updateRemoteVideoTrack(track: RemoteVideoTrack?) {
        val oldTrack: RemoteVideoTrack?
        val renderer: VideoSink?
        synchronized(lock) {
            oldTrack = remoteVideoTrack
            remoteVideoTrack = track
            renderer = remoteVideoRenderer
        }
        if (oldTrack != null && oldTrack !== track && renderer != null) {
            oldTrack.removeRenderer(renderer)
        }
        if (track != null && renderer != null) {
            track.addRenderer(renderer)
        }
        listener?.onRemoteVideoTrackChanged(track)
    }

    private fun tearDownLocked() {
        clearVideoTracks()
        eventCollectJob?.cancel()
        eventCollectJob = null
        val old = room
        room = null
        if (old != null) {
            try {
                old.disconnect()
            } catch (t: Throwable) {
                notifyError(t)
            }
            try {
                old.release()
            } catch (t: Throwable) {
                notifyError(t)
            }
        }
    }

    private fun collectRoomEvents(attempt: Long, r: Room) {
        eventCollectJob = scope.launch {
            try {
                r.events.collect { event ->
                    if (attempt != generation) {
                        return@collect
                    }
                    when (event) {
                        is RoomEvent.TrackSubscribed -> {
                            val track = event.track as? RemoteVideoTrack
                            if (track != null &&
                                event.publication.source == Track.Source.CAMERA
                            ) {
                                updateRemoteVideoTrack(track)
                            }
                        }

                        is RoomEvent.TrackUnsubscribed -> {
                            val track = event.track as? RemoteVideoTrack
                            if (track != null && track === remoteVideoTrack) {
                                updateRemoteVideoTrack(null)
                            }
                        }

                        is RoomEvent.Disconnected -> {
                            // Only the disconnect edge is needed to keep the
                            // transport state in sync. FailedToConnect/Connected/
                            // Reconnecting/Reconnected and unrelated track
                            // events do not change call state.
                            clearVideoTracks()
                            if (state != NewNativeLiveKitState.DISCONNECTED) {
                                state = NewNativeLiveKitState.DISCONNECTED
                                notifyDisconnected()
                            }
                        }

                        else -> {}
                    }
                }
            } catch (t: CancellationException) {
                throw t
            } catch (t: Throwable) {
                if (attempt == generation) notifyError(t)
            }
        }
    }

    private fun overrides(): LiveKitOverrides {
        return LiveKitOverrides(
            audioOptions = AudioOptions(
                // Keep LiveKit a passive transport: the default
                // AudioSwitchHandler would request audio focus, set
                // MODE_IN_COMMUNICATION and drive speaker/earpiece routing —
                // all delegated to Android Telecom by the architecture.
                audioHandler = NoAudioHandler(),
            ),
        )
    }

    private fun notifyConnected() {
        try {
            listener?.onConnected()
        } catch (_: Throwable) {
            // Listener exceptions must not break the transport.
        }
    }

    private fun notifyDisconnected() {
        try {
            listener?.onDisconnected()
        } catch (_: Throwable) {
            // Ignored: listener must not break the transport.
        }
    }

    private fun notifyError(t: Throwable) {
        try {
            listener?.onError(t)
        } catch (_: Throwable) {
            // Ignored: listener must not break the transport.
        }
    }
}
