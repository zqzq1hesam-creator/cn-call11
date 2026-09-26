package com.example.mobile

import android.Manifest
import android.content.Context
import android.content.pm.PackageManager
import androidx.core.content.ContextCompat
import io.livekit.android.LiveKit
import io.livekit.android.LiveKitOverrides
import io.livekit.android.RoomOptions
import io.livekit.android.AudioOptions
import io.livekit.android.audio.NoAudioHandler
import io.livekit.android.events.RoomEvent
import io.livekit.android.events.collect
import io.livekit.android.room.Room
import io.livekit.android.room.track.Track
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

    private fun tearDownLocked() {
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
                    if (event is RoomEvent.Disconnected) {
                        // Only the disconnect edge is needed to keep the
                        // transport state in sync. FailedToConnect/Connected/
                        // Reconnecting/Reconnected and participant/track
                        // events are deliberately ignored here.
                        if (state != NewNativeLiveKitState.DISCONNECTED) {
                            state = NewNativeLiveKitState.DISCONNECTED
                            notifyDisconnected()
                        }
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
