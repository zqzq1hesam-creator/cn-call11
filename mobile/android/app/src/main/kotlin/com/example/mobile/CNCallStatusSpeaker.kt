package com.example.mobile

import android.content.Context
import android.media.AudioAttributes
import android.net.Uri
import android.media.MediaPlayer
import android.os.Handler
import android.os.Looper
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Native pre-call status announcer.
 *
 * The status messages are fixed recordings bundled inside the APK.
 * This avoids any dependency on Google TTS, Samsung TTS, language packs,
 * or network access. Actual call audio remains fully controlled by
 * Telecom/LiveKit.
 */
object CNCallStatusSpeaker {
    private const val TAG = "[CN CALL][STATUS AUDIO]"
    private const val ASSET_PREFIX = "sounds/"

    private val lock = Any()
    private val mainHandler = Handler(Looper.getMainLooper())

    @Volatile
    private var currentPlayer: MediaPlayer? = null

    /**
     * Play one of the bundled status recordings:
     *   offline.mp3
     *   busy.mp3
     *   user_not_found.mp3
     */
    fun speak(
        context: Context,
        callId: String,
        assetFileName: String,
        onComplete: () -> Unit,
    ) {
        val id = callId.trim()
        val fileName = assetFileName.trim()

        if (id.isEmpty() || fileName.isEmpty()) {
            println("$TAG skip empty request call_id=$id")
            onComplete()
            return
        }

        val player = MediaPlayer()
        val completed = AtomicBoolean(false)

        fun finishOnce() {
            if (!completed.compareAndSet(false, true)) return

            synchronized(lock) {
                if (currentPlayer === player) {
                    currentPlayer = null
                }
            }

            try {
                player.stop()
            } catch (_: IllegalStateException) {
            } catch (_: Exception) {
            }

            try {
                player.reset()
            } catch (_: Exception) {
            }

            try {
                player.release()
            } catch (_: Exception) {
            }

            mainHandler.post(onComplete)
        }

        fun failPlayback(message: String) {
            println("$TAG error call_id=$id file=$fileName message=$message")
            finishOnce()
        }

        synchronized(lock) {
            val previous = currentPlayer
            currentPlayer = player

            if (previous != null) {
                println("$TAG replacing previous playback call_id=$id")
                try {
                    previous.stop()
                } catch (_: Exception) {
                }
                try {
                    previous.reset()
                } catch (_: Exception) {
                }
                try {
                    previous.release()
                } catch (_: Exception) {
                }
            }
        }

        try {
            player.setAudioAttributes(
                AudioAttributes.Builder()
                    .setUsage(AudioAttributes.USAGE_MEDIA)
                    .setContentType(AudioAttributes.CONTENT_TYPE_SPEECH)
                    .build(),
            )
            player.setVolume(1.0f, 1.0f)

            player.setOnPreparedListener { preparedPlayer ->
                if (completed.get()) return@setOnPreparedListener

                try {
                    println("$TAG onPrepared call_id=$id file=$fileName")
                    preparedPlayer.start()
                    println("$TAG onStart call_id=$id file=$fileName")
                } catch (error: Exception) {
                    failPlayback(error.toString())
                }
            }

            player.setOnCompletionListener {
                println("$TAG onDone call_id=$id file=$fileName")
                finishOnce()
            }

            player.setOnErrorListener { _, what, extra ->
                failPlayback("what=$what extra=$extra")
                true
            }

            // Use the Android-asset URI instead of openFd(). MP3 files may be
            // compressed by aapt2, in which case AssetManager.openFd() fails because
            // a compressed asset has no stable file descriptor/offset range. The URI
            // data source lets MediaPlayer read the packaged asset directly.
            val assetUri = Uri.parse(
                "file:///android_asset/$ASSET_PREFIX$fileName",
            )
            player.setDataSource(
                context.applicationContext,
                assetUri,
            )

            println("$TAG prepare call_id=$id file=$fileName")
            player.prepareAsync()
        } catch (error: Exception) {
            failPlayback(error.toString())
        }
    }

    fun stop() {
        val player: MediaPlayer?
        synchronized(lock) {
            player = currentPlayer
            currentPlayer = null
        }

        if (player == null) return

        try {
            player.stop()
        } catch (_: Exception) {
        }
        try {
            player.reset()
        } catch (_: Exception) {
        }
        try {
            player.release()
        } catch (_: Exception) {
        }
    }
}
