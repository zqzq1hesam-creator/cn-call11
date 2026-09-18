package com.example.mobile

import android.net.Uri
import android.telecom.Connection
import android.telecom.ConnectionRequest
import android.telecom.ConnectionService
import android.telecom.DisconnectCause
import java.util.UUID

class CNCallConnectionService : ConnectionService() {
    override fun onCreateIncomingConnection(
        connectionManagerPhoneAccount: android.telecom.PhoneAccountHandle,
        request: ConnectionRequest,
    ): Connection? {
        val extras = request.extras
        val callId = extras.getString(EXTRA_CALL_ID)?.trim().orEmpty()
        val callerId = extras.getString(EXTRA_CALLER_ID)?.trim().orEmpty()
        val callerName = extras.getString(EXTRA_CALLER_NAME)?.trim().orEmpty()

        if (callId.isEmpty() || callerId.isEmpty()) {
            return Connection.createFailedConnection(
                DisconnectCause(DisconnectCause.ERROR),
            )
        }

        val connection = CNCallConnection(
            appContext = applicationContext,
            callId = callId,
            incoming = true,
            callerId = callerId,
            callerName = callerName,
            address = request.address ?: Uri.parse("cncall:$callerId"),
        )
        if (!CNCallRegistry.put(
                CNCallRegistry.Entry(callId, connection, incoming = true),
            )
        ) {
            return Connection.createFailedConnection(
                DisconnectCause(DisconnectCause.BUSY),
            )
        }

        connection.beginRinging()

        // Score the engine WHILE the call is ringing — not only on answer —
        // and open the native WebSocket (ownership was already reserved by
        // CallFirebaseService before addNewIncomingCall). This is what makes
        // the remote-ends path work end to end:
        //   - the CALLEE's reject() can deliver a real "call_reject" to the
        //     caller (which closes the caller's ringing UI),
        //   - the CALLER's cancel / ring-timeout ("call_cancelled" / "timeout")
        //     frames are received over the native WS and close THIS ringing UI.
        // Best-effort: if engine init or signaling fails the call still rings,
        // and the answer path retries signaling before sending call_accept.
        if (CNCallEngine.initialize(applicationContext, connection.engineCallbacks)) {
            if (CNCallEngine.startIncoming(callId, callerId, callerName)) {
                CNCallEngine.prepareIncomingSignaling(callId)
            }
        }

        return connection
    }

    override fun onCreateOutgoingConnection(
        connectionManagerPhoneAccount: android.telecom.PhoneAccountHandle,
        request: ConnectionRequest,
    ): Connection? {
        val address = request.address ?: return Connection.createFailedConnection(
            DisconnectCause(DisconnectCause.ERROR),
        )

        // Official Dialer integration: the system sends the dialed CN CALL user
        // id as "tel:<digits>" (or "cncall:<digits>" for the app path). Any
        // other scheme is not a CN CALL address and must never create a
        // successful Connection (SIM/GSM calls keep their own path untouched).
        val scheme = address.scheme?.lowercase().orEmpty()
        if (scheme != "cncall" && scheme != "tel") {
            println(
                "[CN CALL][TELECOM] outgoing refused" +
                    " scheme=${scheme.ifEmpty { "(none)" }}",
            )
            return Connection.createFailedConnection(
                DisconnectCause(DisconnectCause.ERROR),
            )
        }

        val targetId = address.schemeSpecificPart?.trim().orEmpty()
        if (targetId.isEmpty()) {
            return Connection.createFailedConnection(
                DisconnectCause(DisconnectCause.ERROR),
            )
        }

        val ownUserId = NativeCallTokenHelper.restoreUserId(applicationContext).orEmpty()
        if (ownUserId.isNotEmpty() && targetId == ownUserId) {
            println("[CN CALL][TELECOM] outgoing refused self-call target_id=$targetId")
            return Connection.createFailedConnection(
                DisconnectCause(DisconnectCause.ERROR),
            )
        }

        // App path already passes its callId in the extras (created exactly once
        // in Flutter before placeCNCall). The official Dialer carries no callId,
        // so it is created HERE exactly once — no other layer may mint a second
        // Uuid for a Telecom-originated call, and that single id travels
        // unchanged all the way into CNCallEngine/startOutgoing → server → callee.
        val extraCallId = request.extras?.getString(EXTRA_CALL_ID)?.trim().orEmpty()
        val callId = if (extraCallId.isNotEmpty()) {
            extraCallId
        } else {
            UUID.randomUUID().toString()
        }

        // Single-call policy: no native Telecom call may be created while
        // another native connection is live (same or different callId).
        if (CNCallRegistry.hasActiveCall()) {
            println("[CN CALL][TELECOM] outgoing refused call_id=$callId single-call busy")
            return Connection.createFailedConnection(
                DisconnectCause(DisconnectCause.BUSY),
            )
        }

        val connection = CNCallConnection(
            appContext = applicationContext,
            callId = callId,
            incoming = false,
            callerId = targetId,
            callerName = targetId,
            address = address,
        )

        if (!CNCallRegistry.put(
                CNCallRegistry.Entry(callId, connection, incoming = false),
            )
        ) {
            return Connection.createFailedConnection(
                DisconnectCause(DisconnectCause.BUSY),
            )
        }

        connection.beginDialing()

        // Native signaling path only: CNCallEngine.startOutgoing acquires native
        // WS ownership first, then sends the single "call" frame over
        // NativeWebSocketClient → server → callee, and only the callee's
        // call_accept starts LiveKit. No Flutter WebSocket, no RtcCallManager.
        val callbacks = connection.engineCallbacks

        if (!CNCallEngine.initialize(applicationContext, callbacks) ||
            !CNCallEngine.startOutgoing(callId, address.toString())
        ) {
            connection.fail(DisconnectCause.ERROR)
        }

        return connection
    }

    override fun onDestroy() {
        super.onDestroy()
    }

    companion object {
        const val EXTRA_CALL_ID = "com.example.mobile.extra.CALL_ID"
        const val EXTRA_CALLER_ID = "com.example.mobile.extra.CALLER_ID"
        const val EXTRA_CALLER_NAME = "com.example.mobile.extra.CALLER_NAME"
    }
}
