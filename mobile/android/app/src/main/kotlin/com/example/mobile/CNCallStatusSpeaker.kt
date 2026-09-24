package com.example.mobile

import android.content.Context
import android.media.AudioManager
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.speech.tts.TextToSpeech
import android.speech.tts.UtteranceProgressListener
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Native pre-call status announcer.
 *
 * It does not change Telecom state or manually route call audio. It asks the
 * Android TTS engine to use the voice-communication audio attributes.
 */
object CNCallStatusSpeaker {
    private val lock = Any()

    @Volatile
    private var tts: TextToSpeech? = null

    private var initializing = false
    private var pending: PendingRequest? = null

    private data class PendingRequest(
        val callId: String,
        val text: String,
        val onComplete: () -> Unit,
    )

    fun speak(
        context: Context,
        callId: String,
        text: String,
        onComplete: () -> Unit,
    ) {
        val id = callId.trim()
        val phrase = text.trim()
        if (id.isEmpty() || phrase.isEmpty()) {
            onComplete()
            return
        }

        var shouldInitialize = false
        synchronized(lock) {
            pending = PendingRequest(id, phrase, onComplete)

            val current = tts
            if (current != null) {
                speakNowLocked(current, pending!!)
                return
            }

            if (!initializing) {
                initializing = true
                shouldInitialize = true
            }
        }

        if (!shouldInitialize) return

        lateinit var created: TextToSpeech
        created = TextToSpeech(context.applicationContext) { status ->
            val request: PendingRequest?
            val engine: TextToSpeech?

            synchronized(lock) {
                initializing = false
                engine = if (status == TextToSpeech.SUCCESS) created else null
                tts = engine
                request = pending
                pending = null
            }

            if (status != TextToSpeech.SUCCESS || request == null || engine == null) {
                try {
                    created.shutdown()
                } catch (_: Exception) {
                }
                request?.onComplete?.invoke()
                return@TextToSpeech
            }

            synchronized(lock) {
                speakNowLocked(engine, request)
            }
        }
    }

    private fun speakNowLocked(
        engine: TextToSpeech,
        request: PendingRequest,
    ) {
        var language = engine.setLanguage(Locale("ar", "SA"))
        if (language == TextToSpeech.LANG_MISSING_DATA ||
            language == TextToSpeech.LANG_NOT_SUPPORTED
        ) {
            language = engine.setLanguage(Locale("ar"))
        }

        if (language == TextToSpeech.LANG_MISSING_DATA ||
            language == TextToSpeech.LANG_NOT_SUPPORTED
        ) {
            request.onComplete()
            return
        }

        engine.setAudioAttributes(
            android.media.AudioAttributes.Builder()
                .setUsage(android.media.AudioAttributes.USAGE_MEDIA)
                .setContentType(android.media.AudioAttributes.CONTENT_TYPE_SPEECH)
                .build(),
        )

        val utteranceId = "cn-call-status-${request.callId}-${System.nanoTime()}"
        val completed = AtomicBoolean(false)

        fun completeOnce() {
            if (!completed.compareAndSet(false, true)) return
            Handler(Looper.getMainLooper()).post {
                request.onComplete()
            }
        }

        engine.setOnUtteranceProgressListener(
            object : UtteranceProgressListener() {
                override fun onStart(utteranceId: String?) {
                    println("[CN CALL][STATUS TTS] onStart call_id=${request.callId}")
                }

                override fun onDone(utteranceId: String?) {
                    completeOnce()
                }

                override fun onError(utteranceId: String?) {
                    completeOnce()
                }
            },
        )

        val params = Bundle().apply {
            putInt(
                TextToSpeech.Engine.KEY_PARAM_STREAM,
                AudioManager.STREAM_MUSIC,
            )
        }

        if (engine.speak(
                request.text,
                TextToSpeech.QUEUE_FLUSH,
                params,
                utteranceId,
            ) == TextToSpeech.ERROR
        ) {
            completeOnce()
        }
    }
}
