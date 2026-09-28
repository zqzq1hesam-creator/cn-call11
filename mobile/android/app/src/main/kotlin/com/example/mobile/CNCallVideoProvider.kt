package com.example.mobile

import android.net.Uri
import android.os.Handler
import android.os.Looper
import android.telecom.Connection
import android.telecom.VideoProfile
import android.view.Surface
import livekit.org.webrtc.EglBase
import livekit.org.webrtc.GlRectDrawer
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
                    } else {
                        displaySurface = null
                        displayRenderer = renderer
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

            var diagnosticsInstalled = false

            if (renderer == null) {
                val sharedEglContext = NativeLiveKit.getVideoEglBaseContext()
                if (sharedEglContext == null) {
                    println(
                        "[CN CALL][VIDEO PROVIDER] shared EGL context not ready " +
                            "call_id=$callId",
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
                        diagnosticsInstalled = true
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

            if (!diagnosticsInstalled && renderer !== null) {
                // Existing renderer already has its diagnostics listener.
            }

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
                } else {
                    displayRenderer = activeRenderer
                    displaySurface = surface
                }
            }

            if (preview) {
                listener.onPreviewRendererChanged(activeRenderer)
            } else {
                listener.onDisplayRendererChanged(activeRenderer)
            }

            println(
                "[CN CALL][VIDEO PROVIDER] surface " +
                    "${if (preview) "preview" else "display"} updated " +
                    "call_id=$callId present=true",
            )
        }
    }

    private fun sameSurface(first: Surface?, second: Surface): Boolean {
    private fun installRenderDiagnostics(
        renderer: SurfaceEglRenderer,
        label: String,
    ) {
        val renderCount = AtomicInteger(0)
        renderer.addRenderListener(
            SurfaceEglRenderer.RenderListener { timestampNs ->
                val count = renderCount.incrementAndGet()
                if (count <= 3 || count % 60 == 0) {
                    println(
                        "[CN CALL][VIDEO RENDER] " + label +
                            " swapBuffers count=" + count +
                            " ts=" + timestampNs,
                    )
                }
            },
        )
        renderer.setErrorCallback(
            object : SurfaceEglRenderer.ErrorCallback {
                override fun onGlOutOfMemory() {
                    println(
                        "[CN CALL][VIDEO RENDER ERROR] " + label +
                            " GL_OUT_OF_MEMORY",
                    )
                }
            },
        )
    }

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
