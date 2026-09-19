package com.example.mobile

import android.content.Intent
import android.content.ActivityNotFoundException
import android.content.pm.PackageManager
import android.media.AudioAttributes
import android.media.AudioFocusRequest
import android.media.AudioManager
import android.media.Ringtone
import android.media.RingtoneManager
import android.app.NotificationManager
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.provider.Settings
import android.telecom.TelecomManager
import io.flutter.embedding.android.FlutterActivity
import io.flutter.embedding.engine.FlutterEngine
import io.flutter.plugin.common.BinaryMessenger
import io.flutter.plugin.common.MethodChannel
import org.json.JSONObject

/** Hosts the Flutter-owned CN CALL UI; it never registers a Telecom call. */
class MainActivity : FlutterActivity() {
    private var defaultRingtone: Ringtone? = null
    private var audioManager: AudioManager? = null
    private var audioFocusRequest: AudioFocusRequest? = null
    private var pendingCallId: String? = null
    private var pendingTargetId: String? = null
    private var pendingCallResult: MethodChannel.Result? = null
    private var pendingStartupPermissionResult: MethodChannel.Result? = null
    private var startupPermissions = emptyList<String>()
    private var startupPermissionIndex = 0
    companion object {
        private const val EVENTS_CHANNEL = "cn_call/telecom_events"
        private const val PREFERENCES = "FlutterSharedPreferences"
        private const val REQUEST_CALL_PHONE = 9301

        /**
         * The Flutter Engine messenger of the running MainActivity, when the
         * app UI is alive. Used by CNCallConnection to push "active"/"ended"
         * events so the in-app CallScreen tracks the same native call. Best
         * effort: no messenger (system Dialer path / cold start) → no push.
         */
        @Volatile
        private var flutterMessenger: BinaryMessenger? = null

        fun attachFlutterMessenger(messenger: BinaryMessenger) {
            flutterMessenger = messenger
        }

        fun detachFlutterMessenger() {
            flutterMessenger = null
        }

        fun postTelecomEvent(method: String, arguments: Map<String, Any?>) {
            val messenger = flutterMessenger ?: return
            Handler(Looper.getMainLooper()).post {
                try {
                    MethodChannel(messenger, EVENTS_CHANNEL)
                        .invokeMethod(method, arguments)
                } catch (_: Exception) {
                    // Flutter may be mid-teardown; events are best-effort only.
                }
            }
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
    }

    override fun configureFlutterEngine(flutterEngine: FlutterEngine) {
        super.configureFlutterEngine(flutterEngine)
        attachFlutterMessenger(flutterEngine.dartExecutor.binaryMessenger)
        MethodChannel(flutterEngine.dartExecutor.binaryMessenger, "cn_call/call")
            .setMethodCallHandler { call, result ->
                when (call.method) {
                    "presentIncomingCall" -> {
                        val callId = call.argument<String>("callId")?.trim().orEmpty()
                        val callerId = call.argument<String>("callerId")?.trim().orEmpty()
                        val callerName =
                            call.argument<String>("callerName")?.trim().orEmpty()
                                .ifEmpty { "مستخدم CN CALL" }

                        if (callId.isEmpty() || callerId.isEmpty()) {
                            result.error(
                                "missing_incoming_call_identity",
                                "presentIncomingCall requires callId and callerId",
                                null,
                            )
                            return@setMethodCallHandler
                        }

                        try {
                            val acquired =
                                NativeWebSocketClient.tryAcquireNativeOwnership(this)
                            println(
                                "[CN CALL][TELECOM] Flutter incoming handoff" +
                                    " call_id=$callId caller_id=$callerId native_owner=$acquired",
                            )

                            if (!acquired) {
                                result.error(
                                    "native_signaling_busy",
                                    "Native CN CALL signaling ownership is unavailable",
                                    null,
                                )
                                return@setMethodCallHandler
                            }

                            if (!CNCallRegistry.claimTelecomPresentation(callId)) {
                                println(
                                    "[CN CALL][TELECOM] incoming already presented" +
                                        " call_id=$callId",
                                )
                                result.success(true)
                                return@setMethodCallHandler
                            }

                            val telecomManager =
                                getSystemService(TelecomManager::class.java)
                                    ?: run {
                                        CNCallRegistry.releaseTelecomPresentation(callId)
                                        result.error(
                                            "telecom_unavailable",
                                            "Telecom service is unavailable",
                                            null,
                                        )
                                        return@setMethodCallHandler
                                    }

                            CNCallEngine.primeIncomingDeliverySignaling(this)

                            telecomManager.addNewIncomingCall(
                                CNCallPhoneAccount.handle(this),
                                Bundle().apply {
                                    putString(CNCallConnectionService.EXTRA_CALL_ID, callId)
                                    putString(CNCallConnectionService.EXTRA_CALLER_ID, callerId)
                                    putString(CNCallConnectionService.EXTRA_CALLER_NAME, callerName)
                                },
                            )

                            val delivered =
                                CNCallEngine.acknowledgeIncomingCallDelivered(
                                    this,
                                    callId,
                                    callerId,
                                )

                            println(
                                "[CN CALL][TELECOM] Flutter incoming handoff complete" +
                                    " call_id=$callId delivered_ack=$delivered",
                            )
                            result.success(true)
                        } catch (error: Exception) {
                            CNCallRegistry.releaseTelecomPresentation(callId)
                            println(
                                "[CN CALL][TELECOM] Flutter incoming handoff failed" +
                                    " call_id=$callId error=$error",
                            )
                            result.error(
                                "incoming_telecom_failed",
                                "Failed to present incoming CN CALL",
                                error.message,
                            )
                        }
                    }

                    "placeCNCall" -> {
                        val callId = call.argument<String>("callId")?.trim().orEmpty()
                        val targetId = call.argument<String>("targetId")?.trim().orEmpty()

                        if (callId.isEmpty() || targetId.isEmpty()) {
                            result.error(
                                "missing_call_identity",
                                "placeCNCall requires callId and targetId",
                                null,
                            )
                            return@setMethodCallHandler
                        }

                        try {
                            val telecomManager =
                                getSystemService(android.telecom.TelecomManager::class.java)

                            if (telecomManager == null) {
                                result.error(
                                    "telecom_unavailable",
                                    "Telecom service is unavailable",
                                    null,
                                )
                                return@setMethodCallHandler
                            }

                            if (!CNCallPhoneAccount.isEnabled(this)) {
                                result.error(
                                    "phone_account_disabled",
                                    "CN CALL PhoneAccount is not enabled",
                                    null,
                                )
                                return@setMethodCallHandler
                            }

                            if (!hasCallPermission()) {
                                // First in-app call: ask for the runtime
                                // permission the official way. The TRUE
                                // placeCall is only attempted once granted;
                                // "don't send the call before the permission".
                                if (pendingCallResult != null) {
                                    result.error(
                                        "request_in_progress",
                                        "A call permission request is already pending",
                                        null,
                                    )
                                    return@setMethodCallHandler
                                }
                                pendingCallId = callId
                                pendingTargetId = targetId
                                pendingCallResult = result
                                requestPermissions(
                                    arrayOf(
                                        android.Manifest.permission.CALL_PHONE,
                                        android.Manifest.permission.READ_PHONE_NUMBERS,
                                    ),
                                    REQUEST_CALL_PHONE,
                                )
                                return@setMethodCallHandler
                            }

                            placeCNCallWithTelecom(callId, targetId, result)
                        } catch (error: SecurityException) {
                            result.error(
                                "telecom_security",
                                "Telecom rejected outgoing CN CALL",
                                error.message,
                            )
                        } catch (error: Exception) {
                            result.error(
                                "telecom_place_call_failed",
                                "Failed to place outgoing CN CALL",
                                error.message,
                            )
                        }
                    }

                    "registerCNCallPhoneAccount" -> {
                        try {
                            CNCallPhoneAccount.register(this)
                            result.success(true)
                        } catch (error: SecurityException) {
                            result.error(
                                "phone_account_security",
                                "Telecom rejected CN CALL account registration",
                                error.message,
                            )
                        } catch (error: IllegalArgumentException) {
                            result.error(
                                "phone_account_invalid",
                                "CN CALL phone account is invalid",
                                error.message,
                            )
                        }
                    }
                    "isCNCallPhoneAccountEnabled" -> {
                        try {
                            result.success(CNCallPhoneAccount.isEnabled(this))
                        } catch (error: IllegalStateException) {
                            result.error(
                                "telecom_unavailable",
                                "Telecom service is unavailable",
                                error.message,
                            )
                        }
                    }
                    "hasReadPhoneNumbersPermission" -> {
                        result.success(
                            Build.VERSION.SDK_INT < Build.VERSION_CODES.O ||
                                checkSelfPermission(
                                    android.Manifest.permission.READ_PHONE_NUMBERS,
                                ) == PackageManager.PERMISSION_GRANTED,
                        )
                    }
                    "hasStartupPermissions" -> {
                        result.success(hasStartupPermissions())
                    }
                    "requestStartupPermissions" -> {
                        if (pendingStartupPermissionResult != null) {
                            result.error(
                                "request_in_progress",
                                "A startup permission request is already pending",
                                null,
                            )
                            return@setMethodCallHandler
                        }
                        val permissions = buildList {
                            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M &&
                                checkSelfPermission(android.Manifest.permission.RECORD_AUDIO) !=
                                    PackageManager.PERMISSION_GRANTED
                            ) {
                                add(android.Manifest.permission.RECORD_AUDIO)
                            }
                            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O &&
                                checkSelfPermission(android.Manifest.permission.READ_PHONE_NUMBERS) !=
                                    PackageManager.PERMISSION_GRANTED
                            ) {
                                add(android.Manifest.permission.READ_PHONE_NUMBERS)
                            }
                        }
                        if (permissions.isEmpty()) {
                            result.success(true)
                        } else {
                            pendingStartupPermissionResult = result
                            startupPermissions = permissions
                            startupPermissionIndex = 0
                            requestNextStartupPermission()
                        }
                    }
                    "openAppSettings" -> {
                        try {
                            startActivity(
                                Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS).apply {
                                    data = Uri.fromParts("package", packageName, null)
                                    addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                                },
                            )
                            result.success(true)
                        } catch (_: ActivityNotFoundException) {
                            result.success(false)
                        }
                    }
                    "openTelecomCallSettings" -> {
                        // Shows the system "Calling accounts" screen where the
                        // user toggles the CN CALL PhoneAccount on/off.
                        val target =
                            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                                Intent(TelecomManager.ACTION_CHANGE_PHONE_ACCOUNTS)
                            } else {
                                Intent(Settings.ACTION_SETTINGS)
                            }
                        try {
                            startActivity(target)
                            result.success(true)
                        } catch (_: ActivityNotFoundException) {
                            result.success(false)
                        }
                    }
                    "canUseFullScreenIntent" -> {
                        result.success(
                            Build.VERSION.SDK_INT < Build.VERSION_CODES.UPSIDE_DOWN_CAKE ||
                                getSystemService(NotificationManager::class.java)
                                    ?.canUseFullScreenIntent() == true,
                        )
                    }
                    "openFullScreenIntentSettings" -> {
                        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
                            result.success(false)
                        } else {
                            try {
                                startActivity(
                                    Intent(Settings.ACTION_MANAGE_APP_USE_FULL_SCREEN_INTENT).apply {
                                        data = Uri.parse("package:$packageName")
                                    },
                                )
                                result.success(true)
                            } catch (_: ActivityNotFoundException) {
                                result.success(false)
                            }
                        }
                    }
                    "configureCallAudio" -> {
                        stopDefaultRingtone()
                        releaseActivityAudioFocus()
                        result.success(true)
                    }
                    "prepareRingbackAudio" -> {
                          configureCallAudio(false)
                          result.success(true)
                      }
                      "playDefaultRingtone" -> {
                        playDefaultRingtone(call.argument<Boolean>("earpiece") == true)
                        result.success(true)
                    }
                    "stopDefaultRingtone" -> {
                        stopDefaultRingtone()
                        result.success(true)
                    }
                    "endActiveTelecomCall" -> {
                        // App-originated outgoing: the in-app CallScreen ends the
                        // native Telecom call through its Connection (the SINGLE
                        // terminal path — CNCallConnection.onDisconnect →
                        // CNCallEngine.disconnect sends the terminal frame once;
                        // Flutter sends no hangup for this call).
                        val callId = call.argument<String>("callId")?.trim().orEmpty()
                        val entry = if (callId.isEmpty()) {
                            null
                        } else {
                            CNCallRegistry.get(callId)
                        }
                        if (entry == null) {
                            result.success(false)
                        } else {
                            entry.connection.onDisconnect()
                            result.success(true)
                        }
                    }
                    "syncCredentials" -> {
                        val ok = CNCallEngine.bootstrapSignaling(applicationContext)
                        result.success(ok)
                    }
                    "logout" -> {
                        val ok = CNCallEngine.shutdownSignaling(applicationContext)
                        result.success(ok)
                    }
                    "hasActiveCall" -> {
                        result.success(CNCallEngine.hasActiveCall())
                    }
                    else -> result.notImplemented()
                }
            }
    }

    private fun hasCallPermission(): Boolean {
        return Build.VERSION.SDK_INT < Build.VERSION_CODES.M ||
            checkSelfPermission(android.Manifest.permission.CALL_PHONE) ==
                PackageManager.PERMISSION_GRANTED
    }

    private fun hasStartupPermissions(): Boolean {
        val hasAudio = Build.VERSION.SDK_INT < Build.VERSION_CODES.M ||
            checkSelfPermission(android.Manifest.permission.RECORD_AUDIO) ==
                PackageManager.PERMISSION_GRANTED
        val hasPhoneNumbers = Build.VERSION.SDK_INT < Build.VERSION_CODES.O ||
            checkSelfPermission(android.Manifest.permission.READ_PHONE_NUMBERS) ==
                PackageManager.PERMISSION_GRANTED
        return hasAudio && hasPhoneNumbers
    }

    private fun requestNextStartupPermission() {
        if (startupPermissionIndex >= startupPermissions.size) {
            val result = pendingStartupPermissionResult
            pendingStartupPermissionResult = null
            startupPermissions = emptyList()
            startupPermissionIndex = 0
            result?.success(hasStartupPermissions())
            return
        }
        requestPermissions(
            arrayOf(startupPermissions[startupPermissionIndex]),
            9100,
        )
    }

    private fun placeCNCallWithTelecom(
        callId: String,
        targetId: String,
        result: MethodChannel.Result,
    ) {
        try {
            val telecomManager =
                getSystemService(android.telecom.TelecomManager::class.java)
                    ?: run {
                        result.error(
                            "telecom_unavailable",
                            "Telecom service is unavailable",
                            null,
                        )
                        return
                    }
            val address = Uri.parse("cncall:$targetId")
            val extras = Bundle().apply {
                putString(CNCallConnectionService.EXTRA_CALL_ID, callId)
            }
            telecomManager.placeCall(address, extras)
            result.success(true)
        } catch (error: SecurityException) {
            result.error(
                "telecom_security",
                "Telecom rejected outgoing CN CALL",
                error.message,
            )
        } catch (error: Exception) {
            result.error(
                "telecom_place_call_failed",
                "Failed to place outgoing CN CALL",
                error.message,
            )
        }
    }

    override fun onRequestPermissionsResult(
        requestCode: Int,
        permissions: Array<out String>,
        grantResults: IntArray,
    ) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults)
        if (requestCode == 9100) {
            startupPermissionIndex += 1
            requestNextStartupPermission()
            return
        }
        if (requestCode != REQUEST_CALL_PHONE) return

        val callId = pendingCallId
        val targetId = pendingTargetId
        val result = pendingCallResult
        pendingCallId = null
        pendingTargetId = null
        pendingCallResult = null

        if (callId == null || targetId == null || result == null) return

        if (grantResults.isNotEmpty() &&
            grantResults[0] == PackageManager.PERMISSION_GRANTED
        ) {
            placeCNCallWithTelecom(callId, targetId, result)
        } else {
            result.error(
                "permission_denied",
                "CALL_PHONE permission is required to place in-app calls",
                null,
            )
        }
    }

    private fun configureCallAudio(speaker: Boolean) {
        val manager = getSystemService(AUDIO_SERVICE) as AudioManager
        audioManager = manager
        manager.mode = AudioManager.MODE_IN_COMMUNICATION
        manager.isSpeakerphoneOn = speaker
        val focus = AudioFocusRequest.Builder(AudioManager.AUDIOFOCUS_GAIN_TRANSIENT_EXCLUSIVE)
            .setAudioAttributes(
                AudioAttributes.Builder()
                    .setUsage(AudioAttributes.USAGE_VOICE_COMMUNICATION)
                    .setContentType(AudioAttributes.CONTENT_TYPE_SPEECH)
                    .build(),
            )
            .build()
        audioFocusRequest = focus
        manager.requestAudioFocus(focus)
    }

    private fun releaseActivityAudioFocus() {
        audioFocusRequest?.let { audioManager?.abandonAudioFocusRequest(it) }
        audioFocusRequest = null
    }

    private fun playDefaultRingtone(earpiece: Boolean) {
        stopDefaultRingtone()
        configureCallAudio(speaker = !earpiece)
        val uri = RingtoneManager.getActualDefaultRingtoneUri(
            this,
            RingtoneManager.TYPE_RINGTONE,
        ) ?: RingtoneManager.getDefaultUri(RingtoneManager.TYPE_RINGTONE)
        defaultRingtone = RingtoneManager.getRingtone(this, uri)
        defaultRingtone?.apply {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) isLooping = true
            audioAttributes = AudioAttributes.Builder()
                .setUsage(
                    if (earpiece) AudioAttributes.USAGE_VOICE_COMMUNICATION_SIGNALLING
                    else AudioAttributes.USAGE_NOTIFICATION_RINGTONE,
                )
                .setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION)
                .build()
            play()
        }
    }

    private fun stopDefaultRingtone() {
        defaultRingtone?.stop()
        defaultRingtone = null
    }

    override fun onDestroy() {
        detachFlutterMessenger()
        stopDefaultRingtone()
        audioFocusRequest?.let { audioManager?.abandonAudioFocusRequest(it) }
        audioManager?.mode = AudioManager.MODE_NORMAL
        super.onDestroy()
    }

}
