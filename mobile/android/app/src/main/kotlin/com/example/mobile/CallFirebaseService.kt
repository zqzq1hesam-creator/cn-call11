package com.example.mobile

import org.json.JSONArray
import org.json.JSONObject
import android.os.Bundle
import android.telecom.DisconnectCause
import android.telecom.TelecomManager
import com.google.firebase.messaging.FirebaseMessagingService
import com.google.firebase.messaging.RemoteMessage

class CallFirebaseService : FirebaseMessagingService() {

    override fun onMessageReceived(message: RemoteMessage) {

        val type = message.data["type"]

        if (
            type == "call_cancelled" ||
            type == "call_reject" ||
            type == "hangup" ||
            type == "timeout" ||
            type == "disconnected"
        ) {
            val callId =
                message.data["call_id"]
                    ?.trim()
                    .orEmpty()

            if (callId.isEmpty()) return

            try {
                markCallEnded(callId)
            } catch (error: Exception) {
                println(
                    "[CN CALL][FCM] terminal persistence failed " +
                        "call_id=$callId error=$error",
                )
            }
            var terminalHandledLocally = false

            try {
                val connection = CNCallRegistry.get(callId)
                    ?.connection as? CNCallConnection

                if (connection != null) {
                    connection.terminateFromRemote(
                        when (type) {
                            "call_reject" -> DisconnectCause.REJECTED
                            "call_cancelled", "hangup", "disconnected" ->
                                DisconnectCause.REMOTE
                            "timeout" -> DisconnectCause.CANCELED
                            else -> DisconnectCause.REMOTE
                        },
                    )
                }

                // The tombstone is already durably committed above.  A missing
                // process-local Connection is therefore still a valid terminal
                // FCM delivery: a later/native WS duplicate is harmless.
                terminalHandledLocally = true

                println(
                    "[CN CALL][FCM] " +
                        "FCM TERMINAL HANDLED " +
                        "type=$type call_id=$callId",
                )
            } catch (error: Exception) {
                println(
                    "[CN CALL][FCM] terminal handling failed " +
                        "call_id=$callId error=$error",
                )
            }

            // Durable terminal events carry event_id.  The engine owns the
            // native signaling setup so the cold-start FCM path does not call
            // NativeWebSocketClient.send() before it has been configured.
            val eventId = message.data["event_id"]?.trim().orEmpty()
            if (terminalHandledLocally && eventId.isNotEmpty()) {
                try {
                    val sent =
                        CNCallEngine.acknowledgeTerminalEventFromFcm(
                            this,
                            eventId,
                        )

                    println(
                        "[CN CALL][FCM] terminal_ack " +
                            "sent=$sent event_id=$eventId",
                    )
                } catch (error: Exception) {
                    println(
                        "[CN CALL][FCM] terminal_ack failed " +
                            "event_id=$eventId error=$error",
                    )
                }
            }

            return
        }

        if (type != "incoming_call") {
            return
        }

        val callerName =
            message.data["caller_name"]
                ?: "CN CALL"

        val callerId =
            message.data["caller_id"]
                ?: message.data["from_id"]
                ?: ""

        if (callerId.isEmpty()) {
            return
        }

        val callId = message.data["call_id"]?.trim().orEmpty()
        if (callId.isEmpty()) return
        if (isCallEnded(callId)) {
            println("CN CALL: ignored stale incoming FCM. callId=$callId")
            return
        }

        // Phase 3 (cold-start): reserve native signaling ownership NOW, before
        // Telecom presents the call. This closes the window between FCM arrival
        // and the user answering, during which a re-launched Flutter app could
        // otherwise open its own WebSocket and fight for the socket. Reuses the
        // existing shared owner marker via NativeWebSocketClient (the same API
        // CNCallEngine uses); it refuses to seize a genuinely active/ringing
        // Flutter call (flutterHasManagedCall), so a live Flutter call is never
        // hijacked. The native signaling connection is primed after the
        // Telecom presentation slot is reserved, before addNewIncomingCall().
        val acquired = NativeWebSocketClient.tryAcquireNativeOwnership(this)
        println("[CN CALL][FCM] incoming call_id=$callId native_ws_ownership_acquired=$acquired")

        // Phase 4 (foreground): when Flutter itself is already ringing/managing
        // THIS exact call (it reached the call first through its own WebSocket
        // and wrote flutter.cn_call_active_call_id / pending_incoming_call),
        // the same call must not also be presented to Telecom. Presenting it
        // would create a second answer path (system UI) that could send its own
        // call_accept for the same call_id — or fail the system answer because
        // the owner marker is "flutter". Foreground stays Flutter-owned: the
        // Telecom bridge is skipped and Flutter's accept sends call_accept once.
        if (!acquired && isFlutterManagedSameCall(callId)) {
            println("[CN CALL][FCM] incoming call_id=$callId already Flutter-managed; Telecom skipped")
            return
        }

        // Phase WS-ring: a call presented to Telecom once must never be
        // presented a second time — including the (rare) case where the WebSocket
        // invite and the FCM invite for the SAME call_id both reach this app
        // (Online-path WS presentation followed by an FCM copy of the same call).
        // claimTelecomPresentation() atomically awards the slot to whichever side
        // is first; the loser skips. The Offline/cold-start case is unaffected:
        // no WS "call" frame exists there, so the FCM path always wins the slot.
        if (!CNCallRegistry.claimTelecomPresentation(callId)) {
            println("[CN CALL][FCM] incoming call_id=$callId already presented (WS); Telecom skipped")
            return
        }

        // Start the native signaling connection BEFORE Telecom presentation.
        // The actual delivery ACK is still sent only AFTER
        // addNewIncomingCall() succeeds; this merely removes the cold-WS
        // handshake from the critical path between delivery and ringback.
        val signalingPrimed =
            CNCallEngine.primeIncomingDeliverySignaling(this)
        println(
            "[CN CALL][FCM] delivery signaling primed" +
                " call_id=$callId ready=$signalingPrimed",
        )

        try {
            val telecomManager = getSystemService(TelecomManager::class.java)
                ?: error("Telecom service is unavailable")
            telecomManager.addNewIncomingCall(
                CNCallPhoneAccount.handle(this),
                Bundle().apply {
                    putString(CNCallConnectionService.EXTRA_CALL_ID, callId)
                    putString(CNCallConnectionService.EXTRA_CALLER_ID, callerId)
                    putString(CNCallConnectionService.EXTRA_CALLER_NAME, callerName)
                },
            )

            println("[CN CALL][FCM] Telecom incoming submitted call_id=$callId")
            val deliveryAcked =
                CNCallEngine.acknowledgeIncomingCallDelivered(
                    this,
                    callId,
                    callerId,
                )
            println(
                "[CN CALL][FCM] delivery ACK" +
                    " call_id=$callId sent=$deliveryAcked",
            )
        } catch (e: Exception) {
            println("[CN CALL][FCM] incoming launch failed call_id=$callId error=$e")
            CNCallRegistry.releaseTelecomPresentation(callId)
        }

        return
    }

    private fun isCallEnded(callId: String): Boolean {
        val prefs = getSharedPreferences("FlutterSharedPreferences", MODE_PRIVATE)
        return endedCallIds(prefs).contains(callId)
    }

    /**
     * True when this exact call_id is currently owned by the Flutter app:
     * it is the active call Flutter is ringing/managing now
     * (flutter.cn_call_active_call_id) or the call Flutter persisted as pending
     * (flutter.pending_incoming_call). Reads the same shared file and the same
     * fully-qualified keys the Flutter side writes; no new marker.
     */
    private fun isFlutterManagedSameCall(callId: String): Boolean {
        val prefs = getSharedPreferences("FlutterSharedPreferences", MODE_PRIVATE)
        if (prefs.getString("flutter.cn_call_active_call_id", "") == callId) return true
        val pending = prefs.getString("flutter.pending_incoming_call", null)
        if (!pending.isNullOrEmpty()) {
            return try {
                JSONObject(pending).optString("call_id") == callId
            } catch (_: Exception) {
                false
            }
        }
        return false
    }

    private fun markCallEnded(callId: String) {
        val prefs = getSharedPreferences("FlutterSharedPreferences", MODE_PRIVATE)
        val endedIds = endedCallIds(prefs).toMutableList()
        endedIds.remove(callId)
        endedIds.add(callId)
        if (endedIds.size > 32) {
            endedIds.subList(0, endedIds.size - 32).clear()
        }

        val editor = prefs.edit().putString(
            "flutter.cn_call_ended_call_ids_v2",
            JSONArray(endedIds).toString()
        )
        // This is not the cold-start UI handoff.  It only clears a matching
        // Flutter-owned pending invite from an already-running app.
        val pending = prefs.getString("flutter.pending_incoming_call", null)
        val pendingId = try { JSONObject(pending ?: "{}").optString("call_id") } catch (_: Exception) { "" }
        if (pendingId == callId) editor.remove("flutter.pending_incoming_call")
        if (prefs.getString("flutter.cn_call_active_call_id", null) == callId) {
            editor.remove("flutter.cn_call_active_call_id")
            editor.remove("flutter.cn_call_active_call_at")
        }
        if (!editor.commit()) {
            throw IllegalStateException("terminal tombstone commit failed")
        }
    }

    private fun endedCallIds(prefs: android.content.SharedPreferences): List<String> {
        val encoded = prefs.getString("flutter.cn_call_ended_call_ids_v2", "[]")
        return try {
            val values = JSONArray(encoded ?: "[]")
            List(values.length()) { index -> values.optString(index).trim() }
                .filter { it.isNotEmpty() }
        } catch (_: Exception) {
            emptyList()
        }
    }

}
