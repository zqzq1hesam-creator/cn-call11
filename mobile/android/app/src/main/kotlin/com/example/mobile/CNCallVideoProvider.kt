package com.example.mobile

import android.net.Uri
import android.os.Handler
import android.os.Looper
import android.telecom.Connection
import android.telecom.VideoProfile
import android.view.Surface
import java.util.concurrent.CountDownLatch
import livekit.org.webrtc.EglBase
import livekit.org.webrtc.GlRectDrawer
import livekit.org.webrtc.SurfaceEglRenderer
import livekit.org.webrtc.VideoSink

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

    override fun onSendSessionModifyRequest(
        fromProfile: VideoProfile,
        toProfile: VideoProfile,
    ) {
        listener.onSessionModifyRequest(fromProfile, toProfile)
    }

    override fun onSendSessionModifyResponse(responseProfile: VideoProfile) {
        listener.onSessionModifyResponse(responseProfile)
    }

    override fun onSetCamera(cameraId: String) {
        listener.onSetCamera(cameraId)
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

    override fun onRequestCallDataUsage() {
        listener.onRequestCallDataUsage()
    }

    override fun onSetPauseImage(uri: Uri?) {
        listener.onSetPauseImage(uri)
    }

    /**
     * Replace a Telecom-provided Surface with a renderer attached to that
     * surface. Renderer lifecycle is kept on Android's main thread because the
     * WebRTC renderer requires main-thread initialization.
     */
    private fun updateRenderer(surface: Surface?, preview: Boolean) {
        mainHandler.post {
            var rendererToRelease: SurfaceEglRenderer? = null
            val newRenderer = if (surface == null) {
                null
            } else {
                SurfaceEglRenderer(
                    if (preview) "CN CALL Local Video" else "CN CALL Remote Video",
                ).also { renderer ->
                    renderer.init(
                        null,
                        null,
                        EglBase.CONFIG_PLAIN,
                        GlRectDrawer(),
                    )
                    renderer.createEglSurface(surface)
                }
            }

            synchronized(rendererLock) {
                if (preview) {
                    rendererToRelease = previewRenderer
                    previewRenderer = newRenderer
                } else {
                    rendererToRelease = displayRenderer
                    displayRenderer = newRenderer
                }
            }

            val oldRenderer = rendererToRelease
            if (oldRenderer != null) {
                releaseRenderer(oldRenderer)
            }

            if (preview) {
                listener.onPreviewRendererChanged(newRenderer)
            } else {
                listener.onDisplayRendererChanged(newRenderer)
            }

            println(
                "[CN CALL][VIDEO PROVIDER] surface " +
                    "${if (preview) "preview" else "display"} updated " +
                    "call_id=$callId present=${surface != null}",
            )
        }
    }

    fun release() {
        val latch = CountDownLatch(1)
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
            latch.countDown()
        }
        try {
            latch.await()
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    private fun releaseRenderer(renderer: SurfaceEglRenderer) {
        try {
            renderer.release()
        } catch (_: Exception) {
        }
    }
}
