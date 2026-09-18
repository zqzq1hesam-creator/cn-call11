package com.example.mobile

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

/**
 * Delivers native notification actions to the owning Telecom connection.
 */
class CNCallActionReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        val callId = intent.getStringExtra(EXTRA_CALL_ID)?.trim().orEmpty()
        if (callId.isEmpty()) return

        val connection = CNCallRegistry.get(callId)?.connection as? CNCallConnection ?: return
        when (intent.action) {
            ACTION_ANSWER -> connection.onAnswer()
            ACTION_REJECT -> connection.onReject()
            ACTION_DISCONNECT -> connection.onDisconnect()
        }
    }

    companion object {
        const val ACTION_ANSWER = "com.example.mobile.action.ANSWER"
        const val ACTION_REJECT = "com.example.mobile.action.REJECT"
        const val ACTION_DISCONNECT = "com.example.mobile.action.DISCONNECT"
        const val EXTRA_CALL_ID = "com.example.mobile.extra.CALL_ID"
    }
}
