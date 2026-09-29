package com.example.mobile

import android.net.Uri
import android.os.Handler
import android.os.Looper
import android.telecom.Connection
import android.telecom.VideoProfile
import android.view.Surface
import livekit.org.webrtc.EglBase
import livekit.org.webrtc.GlRectDrawer
import livekit.org.webrtc.EglRenderer
import livekit.org.webrtc.SurfaceEglRenderer
import livekit.org.webrtc.ThreadUtils
import livekit.org.webrtc.VideoSink
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

/**
 * Bridges Android Telecom's video controls/surfaces to CN CALL's native
 * LiveKit media transport. Telecom owns the call UI; LiveKit owns the actual
 * audio/video packets.
 */
class CNCallVideoProvider(
    private val callId: String,
    private val listener: Listener,
) : Connection.VideoProvider() {

    interface Listener {
        fun onSessionModifyRequest(fromProfile: VideoProfile, toProfile: VideoProfile)
        fun onSessionModifyResponse(responseProfile: VideoProfile)
        fun onSetCamera(cameraId: String)
        fun onPreviewRendererChanged(renderer: VideoSink?)
        fun onDisplayRendererChanged(renderer: VideoSink?)
        fun onRequestCameraCapabilities()
        fun onSetDeviceOrientation(rotation: Int)
        fun onSetZoom(value: Float)
        fun onRequestCallDataUsage()
        fun onSetPauseImage(uri: Uri?)
    }

    private val mainHandler = Handler(Looper.getMainLooper())
    private val rendererLock = Any()

    private var previewRenderer: SurfaceEglRenderer? = null
    private var displayRenderer: SurfaceEglRenderer? = null
    private var previewSurface: Surface? = null
    private var displaySurface: Surface? = null

    // Samsung can provide the Telecom surface before LiveKit has created its
    // EGL context. Keep that valid surface until LiveKit is ready instead of
    // dropping it and waiting for Samsung to send it again.
    private var pendingPreviewSurface: Surface? = null
    private var pendingDisplaySurface: Surface? = null

    // Samsung InCallUI may keep the video view in WAITING state until the
    // VideoProvider reports that media reception/transmission has started.
    // Reset these one-shot notifications whenever Telecom supplies a new surface.
    private val txStartSent = AtomicInteger(0)
    private val rxResumeSent = AtomicInteger(0)
    private var lastReportedPeerWidth = 0
    private var lastReportedPeerHeight = 0

    override fun onSendSessionModifyRequest(
        fromProfile: VideoProfile,
        toProfile: VideoProfile,
    ) {
        listener.onSessionModifyRequest(fromProfile, toProfile)
    }

    override fun onSendSessionModifyResponse(responseProfile: VideoProfile) {
        listener.onSessionModifyResponse(responseProfile)
    }

    override fun onSetCamera(cameraId: String?) {
        // Telecom reaches this callback from the system VideoProvider handler.
        // A malformed/null camera id or a downstream runtime exception must
        // never escape this callback and crash the CN CALL process.
        val requestedCameraId = cameraId?.trim().orEmpty()
        if (requestedCameraId.isEmpty()) {
            println(
                "[CN CALL][VIDEO PROVIDER] ignoring empty camera id " +
                    "call_id=$callId",
            )
            return
        }

        mainHandler.post {
            try {
                listener.onSetCamera(requestedCameraId)
            } catch (error: Throwable) {
                println(
                    "[CN CALL][VIDEO PROVIDER] camera switch failed " +
                        "call_id=$callId camera_id=$requestedCameraId " +
                        "error=${error.message}",
                )
            }
        }
    }

    override fun onSetPreviewSurface(surface: Surface?) {
        updateRenderer(surface, preview = true)
    }

    override fun onSetDisplaySurface(surface: Surface?) {
        updateRenderer(surface, preview = false)
    }

    override fun onSetDeviceOrientation(rotation: Int) {
        listener.onSetDeviceOrientation(rotation)
    }

    override fun onSetZoom(value: Float) {
        listener.onSetZoom(value)
    }

    override fun onRequestCameraCapabilities() {
        listener.onRequestCameraCapabilities()
    }

    override fun onRequestConnectionDataUsage() {
        listener.onRequestCallDataUsage()
    }

    override fun onSetPauseImage(uri: Uri?) {
        listener.onSetPauseImage(uri)
    }

    /**
     * Called when LiveKit media is ready. Telecom may have supplied its video
     * surface before that point, so retry any valid surface that was queued.
     */
    internal fun onLiveKitReady() {
        mainHandler.post {
            val pendingPreview: Surface?
            val pendingDisplay: Surface?
            synchronized(rendererLock) {
                pendingPreview = pendingPreviewSurface
                pendingDisplay = pendingDisplaySurface
            }

            if (pendingPreview != null) {
                println(
                    "[CN CALL][VIDEO PROVIDER] retry queued preview surface " +
                        "call_id=$callId",
                )
                updateRenderer(pendingPreview, preview = true)
            }

            if (pendingDisplay != null) {
                println(
                    "[CN CALL][VIDEO PROVIDER] retry queued display surface " +
                        "call_id=$callId",
                )
                updateRenderer(pendingDisplay, preview = false)
            }
        }
    }

    /**
     * Replace a Telecom-provided Surface with a renderer attached to that
     * surface. Android Telecom may deliver the same Surface more than once,
     * and may deliver a new Surface before the previous EGL surface has been
     * fully detached. WebRTC's EglRenderer queues createEglSurface() on its
     * render thread, while releaseEglSurface() waits for that thread to stop
     * touching the old Surface. We therefore reuse one renderer per endpoint
     * and synchronously detach the old EGL surface before attaching a new one.
     */
    private fun updateRenderer(surface: Surface?, preview: Boolean) {
        mainHandler.post {
            var renderer: SurfaceEglRenderer?
            val previousSurface: Surface?

            synchronized(rendererLock) {
                renderer = if (preview) previewRenderer else displayRenderer
                previousSurface = if (preview) previewSurface else displaySurface
            }

            // A new Telecom surface starts a new presentation session.
            if (preview) {
                txStartSent.set(0)
            } else {
                rxResumeSent.set(0)
                synchronized(rendererLock) {
                    lastReportedPeerWidth = 0
                    lastReportedPeerHeight = 0
                }
            }

            // Samsung can repeat the exact same Surface while entering video
            // mode. Do not ask WebRTC to create a second EGL window surface.
            if (
                surface != null &&
                renderer != null &&
                sameSurface(previousSurface, surface)
            ) {
                println(
                    "[CN CALL][VIDEO PROVIDER] surface unchanged " +
                        "${if (preview) "preview" else "display"} " +
                        "call_id=$callId",
                )
                return@post
            }

            // Never detach a valid current surface just because Telecom sent
            // an invalid replacement surface.
            if (surface != null && !surface.isValid) {
                println(
                    "[CN CALL][VIDEO PROVIDER] ignoring invalid " +
                        "${if (preview) "preview" else "display"} surface " +
                        "call_id=$callId",
                )
                return@post
            }

            if (renderer != null) {
                if (!releaseEglSurfaceBlocking(renderer!!)) {
                    // The renderer is no longer safe to reuse if EGL detach
                    // failed. Fully release it before creating a replacement.
                    releaseRenderer(renderer!!)
                    renderer = null
                }
            }

            if (surface == null) {
                synchronized(rendererLock) {
                    if (preview) {
                        previewSurface = null
                        previewRenderer = renderer
                        pendingPreviewSurface = null
                    } else {
                        displaySurface = null
                        displayRenderer = renderer
                        pendingDisplaySurface = null
                    }
                }

                if (preview) {
                    listener.onPreviewRendererChanged(null)
                } else {
                    listener.onDisplayRendererChanged(null)
                }

                println(
                    "[CN CALL][VIDEO PROVIDER] surface " +
                        "${if (preview) "preview" else "display"} cleared " +
                        "call_id=$callId",
                )
                return@post
            }

            if (renderer == null) {
                val sharedEglContext = NativeLiveKit.getVideoEglBaseContext()
                if (sharedEglContext == null) {
                    synchronized(rendererLock) {
                        if (preview) {
                            pendingPreviewSurface = surface
                        } else {
                            pendingDisplaySurface = surface
                        }
                    }
                    println(
                        "[CN CALL][VIDEO PROVIDER] shared EGL context not ready; " +
                            "surface queued endpoint=" +
                            (if (preview) "preview" else "display") +
                            " call_id=$callId",
                    )
                    return@post
                }

                try {
                    renderer = SurfaceEglRenderer(
                        if (preview) {
                            "CN CALL Local Video"
                        } else {
                            "CN CALL Remote Video"
                        },
                    ).also { newRenderer ->
                        newRenderer.init(
                            sharedEglContext,
                            null,
                            EglBase.CONFIG_PLAIN,
                            GlRectDrawer(),
                        )
                        installRenderDiagnostics(
                            newRenderer,
                            if (preview) "LOCAL" else "REMOTE",
                        )
                    }
                } catch (error: Throwable) {
                    println(
                        "[CN CALL][VIDEO PROVIDER] renderer init failed " +
                            "call_id=$callId " +
                            "error=${error.message}",
                    )
                    return@post
                }
            }

            val activeRenderer = renderer ?: return@post

            try {
                // createEglSurface() is asynchronous, but the previous EGL
                // surface has already been detached above, so this Surface
                // cannot be double-connected by our old renderer.
                activeRenderer.createEglSurface(surface)
            } catch (error: Throwable) {
                println(
                    "[CN CALL][VIDEO PROVIDER] renderer surface bind failed " +
                        "call_id=$callId " +
                        "error=${error.message}",
                )
                releaseRenderer(activeRenderer)
                return@post
            }

            synchronized(rendererLock) {
                if (preview) {
                    previewRenderer = activeRenderer
                    previewSurface = surface
                    pendingPreviewSurface = null
                } else {
                    displayRenderer = activeRenderer
                    displaySurface = surface
                    pendingDisplaySurface = null
                }
            }

            if (preview) {
                listener.onPreviewRendererChanged(activeRenderer)
            } else {
                listener.onDisplayRendererChanged(
                    createDimensionReportingSink(activeRenderer),
                )
            }

            println(
                "[CN CALL][VIDEO PROVIDER] surface " +
                    "${if (preview) "preview" else "display"} updated " +
                    "call_id=$callId present=true",
            )
        }
    }

    private fun installRenderDiagnostics(
        renderer: SurfaceEglRenderer,
        label: String,
    ) {
        val renderCount = AtomicInteger(0)
        val frameListener = object : EglRenderer.FrameListener {
            override fun onFrame(frame: android.graphics.Bitmap?) {
                val count = renderCount.incrementAndGet()

                if (label == "REMOTE" && rxResumeSent.compareAndSet(0, 1)) {
                    try {
                        handleCallSessionEvent(
                            Connection.VideoProvider.SESSION_EVENT_RX_RESUME,
                        )
                        println(
                            "[CN CALL][VIDEO PROVIDER] SESSION_EVENT_RX_RESUME " +
                                "call_id=" + callId,
                        )
                    } catch (throwable: Throwable) {
                        rxResumeSent.set(0)
                        println(
                            "[CN CALL][VIDEO PROVIDER] RX_RESUME failed " +
                                "call_id=" + callId +
                                " error=" + throwable.message,
                        )
                    }
                }

                if (label == "LOCAL" && txStartSent.compareAndSet(0, 1)) {
                    try {
                        // Notify Telecom as soon as the first local frame reaches
                        // the renderer callback. Older Samsung InCallUI builds
                        // can keep the preview surface visually inactive until
                        // this transmission-start event has been received.
                        handleCallSessionEvent(
                            Connection.VideoProvider.SESSION_EVENT_TX_START,
                        )
                        println(
                            "[CN CALL][VIDEO PROVIDER] SESSION_EVENT_TX_START " +
                                "call_id=" + callId +
                                " source=first_local_frame",
                        )
                    } catch (throwable: Throwable) {
                        txStartSent.set(0)
                        println(
                            "[CN CALL][VIDEO PROVIDER] TX_START failed " +
                                "call_id=" + callId +
                                " error=" + throwable.message,
                        )
                    }
                }

                if (count <= 3 || count % 60 == 0) {
                    println(
                        "[CN CALL][VIDEO RENDER] " + label +
                            " completed count=" + count,
                    )
                }
                renderer.addFrameListener(this, 0f)
            }
        }

        renderer.addFrameListener(frameListener, 0f)
        renderer.setErrorCallback(
            object : EglRenderer.ErrorCallback {
                override fun onGlOutOfMemory() {
                    println(
                        "[CN CALL][VIDEO RENDER ERROR] " + label +
                            " GL_OUT_OF_MEMORY",
                    )
                }
            },
        )
    }

    private fun createDimensionReportingSink(
        delegate: VideoSink,
    ): VideoSink {
        return object : VideoSink {
            override fun onFrame(frame: livekit.org.webrtc.VideoFrame) {
                reportPeerDimensions(
                    frame.rotatedWidth,
                    frame.rotatedHeight,
                )
                delegate.onFrame(frame)
            }
        }
    }

    /**
     * Reports dimensions from the actual LiveKit VideoFrame path.
     *
     * EglRenderer.FrameListener exposes a Bitmap only after the renderer has
     * accepted the frame, and that Bitmap is not guaranteed to be available
     * for every rendered frame. Telecom/Samsung needs the peer dimensions
     * independently of that diagnostic callback.
     */
    internal fun reportPeerDimensions(width: Int, height: Int) {
        if (width <= 0 || height <= 0) return

        var shouldReport = false
        synchronized(rendererLock) {
            if (
                width != lastReportedPeerWidth ||
                height != lastReportedPeerHeight
            ) {
                lastReportedPeerWidth = width
                lastReportedPeerHeight = height
                shouldReport = true
            }
        }

        if (!shouldReport) return

        mainHandler.post {
            if (width <= 0 || height <= 0) return@post
            try {
                changePeerDimensions(width, height)
                println(
                    "[CN CALL][VIDEO PROVIDER] peer dimensions " +
                        width + "x" + height +
                        " call_id=" + callId,
                )
            } catch (throwable: Throwable) {
                println(
                    "[CN CALL][VIDEO PROVIDER] peer dimensions failed " +
                        "call_id=" + callId +
                        " error=" + throwable.message,
                )
            }
        }
    }

    private fun sameSurface(first: Surface?, second: Surface): Boolean {
        if (first == null) return false
        return first === second || first == second
    }

    /**
     * WebRTC explicitly guarantees that releaseEglSurface() does not return
     * until the render thread has stopped touching the old Surface.
     */
    private fun releaseEglSurfaceBlocking(renderer: SurfaceEglRenderer): Boolean {
        return try {
            val completionLatch = CountDownLatch(1)
            renderer.releaseEglSurface {
                completionLatch.countDown()
            }
            ThreadUtils.awaitUninterruptibly(completionLatch)
            true
        } catch (error: Throwable) {
            println(
                "[CN CALL][VIDEO PROVIDER] EGL surface release failed " +
                    "call_id=$callId error=${error.message}",
            )
            false
        }
    }

    fun release() {
        mainHandler.post {
            val oldPreview: SurfaceEglRenderer?
            val oldDisplay: SurfaceEglRenderer?
            synchronized(rendererLock) {
                oldPreview = previewRenderer
                oldDisplay = displayRenderer
                previewRenderer = null
                displayRenderer = null
                previewSurface = null
                displaySurface = null
                pendingPreviewSurface = null
                pendingDisplaySurface = null
            }
            if (oldPreview != null) {
                listener.onPreviewRendererChanged(null)
                releaseRenderer(oldPreview)
            }
            if (oldDisplay != null) {
                listener.onDisplayRendererChanged(null)
                releaseRenderer(oldDisplay)
            }
        }
    }

    private fun releaseRenderer(renderer: SurfaceEglRenderer) {
        try {
            renderer.release()
        } catch (_: Exception) {
        }
    }
}
