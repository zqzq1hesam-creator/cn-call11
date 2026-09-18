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

    fun cancel(context: Context, callId: String) {
        context.getSystemService(NotificationManager::class.java)
            ?.cancel(notificationId(callId))
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

    private fun notificationId(callId: String): Int = callId.hashCode()

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
