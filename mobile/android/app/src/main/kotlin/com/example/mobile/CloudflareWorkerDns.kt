package com.example.mobile

import okhttp3.Dns
import java.net.InetAddress
import java.net.UnknownHostException

/**
 * DNS policy for CN CALL's Cloudflare Worker only.
 *
 * First tries the device's normal resolver. If the worker hostname is blocked
 * or incorrectly answered by the network, use the known Cloudflare anycast
 * addresses that were explicitly verified against this Worker. Other hosts
 * continue to use OkHttp's system DNS unchanged.
 *
 * OkHttp still uses the original URL hostname for TLS SNI and certificate
 * verification; this resolver changes only the destination IP.
 */
internal object CloudflareWorkerDns : Dns {
    private const val WORKER_HOST = "cn-call11.zqzq1hesam-cncall.workers.dev"

    private val fallbackAddresses = listOf(
        "172.67.136.179",
        "104.21.62.147",
    )

    override fun lookup(hostname: String): List<InetAddress> {
        if (!hostname.equals(WORKER_HOST, ignoreCase = true)) {
            return Dns.SYSTEM.lookup(hostname)
        }

        try {
            val systemAddresses = Dns.SYSTEM.lookup(hostname)
            if (systemAddresses.isNotEmpty()) return systemAddresses
        } catch (systemError: UnknownHostException) {
            val fallback = fallbackAddresses.mapNotNull { address ->
                try {
                    InetAddress.getByName(address)
                } catch (_: Exception) {
                    null
                }
            }

            if (fallback.isNotEmpty()) return fallback

            throw UnknownHostException(
                "Unable to resolve CN CALL Worker by DNS or fallback addresses"
            ).also { it.initCause(systemError) }
        }

        throw UnknownHostException("No IP addresses returned for CN CALL Worker")
    }
}
