package com.example.mobile

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.os.Build

/**
 * Native fallback notification for system-managed incoming calls.
 *
 * Telecom does not provide ringtone or notification UI for a ConnectionService.
 * This class deliberately contains no media or Flutter lifecycle.
 */
object CNCallNotification {
    private const val CHANNEL_ID = "cn_call_incoming"
    private const val MISSED_CHANNEL_ID = "cn_call_missed"
    private const val MISSED_PREFS = "CNCallMissedNotifications"
    private const val MISSED_IDS_KEY = "shown_call_ids"

    fun showIncoming(context: Context, callId: String, callerName: String) {
        val manager = context.getSystemService(NotificationManager::class.java) ?: return
        ensureChannel(manager)
        val notificationBuilder = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            Notification.Builder(context, CHANNEL_ID)
        } else {
            Notification.Builder(context)
        }
        val notification = notificationBuilder
            .setSmallIcon(android.R.mipmap.sym_def_app_icon)
            .setContentTitle("CN CALL")
            .setContentText(callerName.ifBlank { "Incoming call" })
            .setCategory(Notification.CATEGORY_CALL)
            .setPriority(Notification.PRIORITY_HIGH)
            .setOngoing(true)
            .setAutoCancel(false)
            .addAction(
                Notification.Action.Builder(
                    null,
                    "Answer",
                    actionIntent(context, CNCallActionReceiver.ACTION_ANSWER, callId),
                ).build(),
            )
            .addAction(
                Notification.Action.Builder(
                    null,
                    "Reject",
                    actionIntent(context, CNCallActionReceiver.ACTION_REJECT, callId),
                ).build(),
            )
            .build()
        manager.notify(notificationId(callId), notification)
    }

    fun showMissed(context: Context, callId: String, callerName: String) {
        val id = callId.trim()
        if (id.isEmpty()) return
        if (!markMissedAsShown(context, id)) return

        val manager = context.getSystemService(NotificationManager::class.java) ?: return
        ensureMissedChannel(manager)

        val builder = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            Notification.Builder(context, MISSED_CHANNEL_ID)
        } else {
            Notification.Builder(context)
        }

        builder
            .setSmallIcon(android.R.mipmap.sym_def_app_icon)
            .setContentTitle("CN CALL")
            .setContentText(
                "مكالمة فائتة من " +
                    callerName.ifBlank { "مستخدم CN CALL" },
            )
            .setCategory(Notification.CATEGORY_MISSED_CALL)
            .setPriority(Notification.PRIORITY_DEFAULT)
            .setAutoCancel(true)
            .build()
            .also { notification ->
                manager.notify(missedNotificationId(id), notification)
            }

        MainActivity.postTelecomEvent(
            "missedCall",
            mapOf(
                "callId" to id,
                "callerName" to callerName,
            ),
        )
    }

    fun cancel(context: Context, callId: String) {
        context.getSystemService(NotificationManager::class.java)
            ?.cancel(notificationId(callId))
    }

    private fun markMissedAsShown(context: Context, callId: String): Boolean {
        val prefs = context.getSharedPreferences(MISSED_PREFS, Context.MODE_PRIVATE)
        synchronized(prefs) {
            val encoded = prefs.getString(MISSED_IDS_KEY, "").orEmpty()
            val ids = encoded
                .split('|')
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .toMutableList()

            if (ids.contains(callId)) return false

            ids.add(callId)
            if (ids.size > 128) {
                ids.subList(0, ids.size - 128).clear()
            }

            prefs.edit()
                .putString(MISSED_IDS_KEY, ids.joinToString("|"))
                .apply()
            return true
        }
    }

    private fun ensureChannel(manager: NotificationManager) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O &&
            manager.getNotificationChannel(CHANNEL_ID) == null
        ) {
            manager.createNotificationChannel(
                NotificationChannel(
                    CHANNEL_ID,
                    "CN CALL incoming calls",
                    NotificationManager.IMPORTANCE_HIGH,
                ),
            )
        }
    }

    private fun ensureMissedChannel(manager: NotificationManager) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O &&
            manager.getNotificationChannel(MISSED_CHANNEL_ID) == null
        ) {
            manager.createNotificationChannel(
                NotificationChannel(
                    MISSED_CHANNEL_ID,
                    "CN CALL missed calls",
                    NotificationManager.IMPORTANCE_DEFAULT,
                ),
            )
        }
    }

    private fun notificationId(callId: String): Int = callId.hashCode()

    private fun missedNotificationId(callId: String): Int =
        100000 + (callId.hashCode() and 0x7fffffff)

    private fun actionIntent(context: Context, action: String, callId: String): PendingIntent {
        val intent = Intent(context, CNCallActionReceiver::class.java)
            .setAction(action)
            .putExtra(CNCallActionReceiver.EXTRA_CALL_ID, callId)
        return PendingIntent.getBroadcast(
            context,
            31 * callId.hashCode() + action.hashCode(),
            intent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
        )
    }
}
