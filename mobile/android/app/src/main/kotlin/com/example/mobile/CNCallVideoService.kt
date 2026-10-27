package com.example.mobile

import android.app.ForegroundServiceStartNotAllowedException
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder

/**
 * Foreground-service companion used only while CN CALL camera video is active.
 * The existing audio foreground service remains unchanged.
 */
class CNCallVideoService : Service() {
    override fun onCreate() {
        super.onCreate()
        createChannel()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val notificationBuilder = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            Notification.Builder(this, CHANNEL_ID)
        } else {
            Notification.Builder(this)
        }

        val notification = notificationBuilder
            .setSmallIcon(android.R.mipmap.sym_def_app_icon)
            .setContentTitle("CN CALL")
            .setContentText("Video call in progress")
            .setCategory(Notification.CATEGORY_CALL)
            .setOngoing(true)
            .build()

        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                startForeground(
                    NOTIFICATION_ID,
                    notification,
                    ServiceInfo.FOREGROUND_SERVICE_TYPE_CAMERA or
                        ServiceInfo.FOREGROUND_SERVICE_TYPE_MICROPHONE,
                )
            } else {
                startForeground(NOTIFICATION_ID, notification)
            }
        } catch (error: SecurityException) {
            println("[CN CALL][VIDEO SERVICE] startForeground denied: ${error.message}")
            stopSelf()
        } catch (error: IllegalArgumentException) {
            println("[CN CALL][VIDEO SERVICE] invalid foreground type: ${error.message}")
            stopSelf()
        } catch (error: ForegroundServiceStartNotAllowedException) {
            println("[CN CALL][VIDEO SERVICE] background start denied: ${error.message}")
            stopSelf()
        }

        return START_NOT_STICKY
    }

    override fun onBind(intent: Intent?): IBinder? = null

    companion object {
        private const val CHANNEL_ID = "cn_call_video"
        private const val NOTIFICATION_ID = 0x434e56
        private const val EXTRA_CALL_ID = "com.example.mobile.extra.VIDEO_CALL_ID"

        fun startForCall(context: Context, callId: String): Boolean {
            val id = callId.trim()
            if (id.isEmpty()) return false

            val intent = Intent(context, CNCallVideoService::class.java)
                .putExtra(EXTRA_CALL_ID, id)

            return try {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                    context.startForegroundService(intent)
                } else {
                    context.startService(intent)
                }
                true
            } catch (error: SecurityException) {
                println(
                    "[CN CALL][VIDEO SERVICE] start denied " +
                        "call_id=$id error=${error.message}",
                )
                false
            } catch (error: IllegalStateException) {
                println(
                    "[CN CALL][VIDEO SERVICE] start invalid " +
                        "call_id=$id error=${error.message}",
                )
                false
            } catch (error: ForegroundServiceStartNotAllowedException) {
                println(
                    "[CN CALL][VIDEO SERVICE] background start denied " +
                        "call_id=$id error=${error.message}",
                )
                false
            }
        }

        fun stopForCall(context: Context, callId: String) {
            val id = callId.trim()
            if (id.isEmpty()) return
            context.stopService(
                Intent(context, CNCallVideoService::class.java)
                    .putExtra(EXTRA_CALL_ID, id),
            )
        }
    }

    private fun createChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            getSystemService(NotificationManager::class.java)?.createNotificationChannel(
                NotificationChannel(
                    CHANNEL_ID,
                    "CN CALL video",
                    NotificationManager.IMPORTANCE_LOW,
                ),
            )
        }
    }
}
