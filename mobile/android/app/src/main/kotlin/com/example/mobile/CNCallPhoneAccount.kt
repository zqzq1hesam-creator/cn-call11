package com.example.mobile

import android.content.ComponentName
import android.content.Context
import android.telecom.PhoneAccount
import android.telecom.PhoneAccountHandle
import android.telecom.TelecomManager

/**
 * Registers the system-managed CN CALL calling account.
 *
 * Registration is explicit and is not invoked automatically at app startup.
 */
object CNCallPhoneAccount {
    private const val ACCOUNT_ID = "cn_call_provider"

    fun handle(context: Context): PhoneAccountHandle {
        return PhoneAccountHandle(
            ComponentName(
                context.applicationContext,
                CNCallConnectionService::class.java,
            ),
            ACCOUNT_ID,
        )
    }

    fun register(context: Context) {
        val appContext = context.applicationContext
        val account = PhoneAccount.builder(
            handle(appContext),
            "CN CALL",
        )
            // A pure call-provider account (never a SIM): the official Dialer
            // offers it as an independent account for tel: addresses and the
            // app places "cncall:" addresses. It shares nothing with and never
            // interferes with SIM 1 / SIM 2 cellular calls.
            .setCapabilities(PhoneAccount.CAPABILITY_CALL_PROVIDER)
            .setSupportedUriSchemes(listOf("cncall", "tel"))
            .build()

        telecomManager(appContext).registerPhoneAccount(account)
    }

    fun isEnabled(context: Context): Boolean {
        val appContext = context.applicationContext
        return telecomManager(appContext)
            .getPhoneAccount(handle(appContext))
            ?.isEnabled == true
    }

    private fun telecomManager(context: Context): TelecomManager {
        return requireNotNull(
            context.getSystemService(TelecomManager::class.java),
        ) {
            "Telecom service is unavailable"
        }
    }
}
