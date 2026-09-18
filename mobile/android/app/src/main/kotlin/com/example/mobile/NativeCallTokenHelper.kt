package com.example.mobile

import android.content.Context
import android.content.SharedPreferences
import android.os.Looper
import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONObject
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Native credentials + LiveKit token helper for CN CALL.
 *
 * PHASE 1.5-C: ACTIVE. Reads the CN CALL session identity from the same
 * SharedPreferences store the Flutter `shared_preferences` plugin uses (file
 * "FlutterSharedPreferences", keys prefixed with "flutter."), and fetches a
 * LiveKit token from the exact endpoint the Dart path uses. Client shape:
 * mobile/lib/services/livekit_token_service.dart; authoritative contract:
 * server/main.py /livekit/token (line 828).
 *
 * SharedPreferences keys (written by CallSession.login()/restoreSession()):
 *   - "flutter.cn_call_user_id"
 *   - "flutter.cn_call_access_token"
 *   - "flutter.cn_call_display_name" (stored, but not needed by this helper)
 *
 * LiveKit token endpoint:
 *   GET https://cn-call10-production.up.railway.app/livekit/token
 *       ?user_id=<userId>&call_id=<callId>
 *   Authorization: Bearer <accessToken>
 *   Success: 200 {"success": true, "url": "<LIVEKIT_URL>",
 *                 "token": "<jwt>", "room": "call-<callId>"}
 *   Errors:  401 (missing/invalid token), 409 (unknown/ended call or user not
 *            a participant or call not in {accepted,negotiating,connected}),
 *            500 (LiveKit not configured). All map to null here.
 *
 * Caller contract: blocking (returns a value synchronously). The HTTP request
 * is executed on a background worker so the main thread never performs I/O;
 * the engine is expected to call this from a Telecom/Binder worker thread.
 */
object NativeCallTokenHelper {

    // Same storage the Flutter shared_preferences plugin and the existing
    // native code (MainActivity.kt, CallFirebaseService.kt) read from.
    private const val PREFS_NAME = "FlutterSharedPreferences"
    private const val KEY_USER_ID = "flutter.cn_call_user_id"
    private const val KEY_ACCESS_TOKEN = "flutter.cn_call_access_token"

    // Mirrors ServerConfig.host (mobile/lib/services/server_config.dart).
    private const val HOST = "cn-call10-production.up.railway.app"

    private val httpClient: OkHttpClient by lazy {
        OkHttpClient.Builder()
            .connectTimeout(15, TimeUnit.SECONDS)
            .readTimeout(15, TimeUnit.SECONDS)
            .callTimeout(20, TimeUnit.SECONDS)
            // No retry: a failed fetch must surface as null to the engine.
            .retryOnConnectionFailure(false)
            .build()
    }

    private val tokenExecutor: ExecutorService by lazy {
        Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "cn-call-token").apply { isDaemon = true }
        }
    }

    /** Result of a LiveKit token fetch. */
    data class LiveKitTokenResult(
        val url: String,
        val token: String,
        val room: String,
    )

    /**
     * Reads the stored CN CALL access token (flutter.cn_call_access_token).
     * Returns null when absent or blank.
     */
    fun restoreAccessToken(context: Context): String? {
        return prefs(context).getString(KEY_ACCESS_TOKEN, null)?.takeIf { it.isNotEmpty() }
    }

    /**
     * Reads the stored CN CALL user id (flutter.cn_call_user_id).
     * Returns null when absent or blank.
     */
    fun restoreUserId(context: Context): String? {
        return prefs(context).getString(KEY_USER_ID, null)?.takeIf { it.isNotEmpty() }
    }

    /**
     * Fetches a LiveKit token for [userId]/[callId] exactly like the Dart path:
     * GET /livekit/token?user_id=<userId>&call_id=<callId> with the stored
     * access token in "Authorization: Bearer <token>".
     *
     * Returns null when the credentials are absent, the server does not answer
     * 2xx, or the JSON body lacks the required non-empty "url", "token",
     * "room" fields (2xx alone is never treated as a success).
     */
    fun fetchLiveKitToken(
        context: Context,
        userId: String,
        callId: String,
    ): LiveKitTokenResult? {
        val accessToken = restoreAccessToken(context) ?: return null
        if (userId.isBlank() || callId.isBlank()) return null

        val url = HttpUrl.Builder()
            .scheme("https")
            .host(HOST)
            .addPathSegments("livekit/token")
            .addQueryParameter("user_id", userId)
            .addQueryParameter("call_id", callId)
            .build()

        return runTokenFetch { fetch(url = url, accessToken = accessToken) }
    }

    private fun fetch(url: HttpUrl, accessToken: String): LiveKitTokenResult? {
        return try {
            val request = Request.Builder()
                .url(url)
                .header("Authorization", "Bearer $accessToken")
                .build()

            httpClient.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    return@use null
                }
                val body = response.body?.string() ?: return@use null
                val json = JSONObject(body)
                val success = json.optBoolean("success", false)
                val livekitUrl = json.optString("url", "")
                val livekitToken = json.optString("token", "")
                val room = json.optString("room", "")
                if (!success ||
                    livekitUrl.isEmpty() ||
                    livekitToken.isEmpty() ||
                    room.isEmpty()
                ) {
                    return@use null
                }
                LiveKitTokenResult(livekitUrl, livekitToken, room)
            }
        } catch (e: Exception) {
            null
        }
    }

    private fun prefs(context: Context): SharedPreferences {
        return context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
    }

    /**
     * Executes the token request on a background worker when invoked on the
     * main thread; runs inline when already off the main thread. The caller
     * still receives a synchronous result.
     */
    private fun runTokenFetch(block: () -> LiveKitTokenResult?): LiveKitTokenResult? {
        if (Looper.myLooper() == Looper.getMainLooper()) {
            val future = tokenExecutor.submit(Callable { block() })
            return try {
                future.get()
            } catch (e: Exception) {
                future.cancel(true)
                null
            }
        }
        return block()
    }
}
