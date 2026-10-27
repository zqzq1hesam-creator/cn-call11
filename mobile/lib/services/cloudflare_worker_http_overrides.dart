import 'dart:io';

const String _cnCallWorkerHost =
    'cn-call11.zqzq1hesam-cncall.workers.dev';

// These two Cloudflare anycast addresses were verified against this Worker
// with the correct Host/SNI and a valid TLS certificate. They are only a
// fallback when the device's DNS resolver cannot resolve workers.dev.
const List<String> _cnCallWorkerIpv4Fallbacks = <String>[
  '172.67.136.179',
  '104.21.62.147',
];

/// Installs a DNS fallback for this Worker only. TLS still validates and sends
/// SNI for the real workers.dev hostname; certificate verification is unchanged.
void installCloudflareWorkerHttpOverrides() {
  HttpOverrides.global = _CloudflareWorkerHttpOverrides();
}

class _CloudflareWorkerHttpOverrides extends HttpOverrides {
  @override
  HttpClient createHttpClient(SecurityContext? context) {
    final client = super.createHttpClient(context);
    client.connectionFactory = (
      Uri uri,
      String? proxyHost,
      int? proxyPort,
    ) {
      // Preserve explicitly configured proxies.
      if (proxyHost != null && proxyPort != null) {
        return Socket.startConnect(proxyHost, proxyPort);
      }

      if (uri.host.toLowerCase() == _cnCallWorkerHost) {
        return Future<ConnectionTask<Socket>>.value(
          ConnectionTask<Socket>.fromSocket(
            _connectWorker(uri.port),
            () {},
          ),
        );
      }

      // Use normal DNS for every other host.
      return Socket.startConnect(uri.host, uri.port);
    };
    return client;
  }

  Future<Socket> _connectWorker(int port) async {
    Object? lastError;

    // Prefer ordinary DNS whenever it works.
    try {
      return await Socket.connect(
        _cnCallWorkerHost,
        port,
        timeout: const Duration(seconds: 3),
      );
    } catch (error) {
      lastError = error;
    }

    // Fall back to verified Cloudflare anycast IPs without disabling TLS
    // certificate verification or changing the request hostname.
    for (final address in _cnCallWorkerIpv4Fallbacks) {
      try {
        return await Socket.connect(
          InternetAddress(address),
          port,
          timeout: const Duration(seconds: 4),
        );
      } catch (error) {
        lastError = error;
      }
    }

    throw SocketException(
      'Unable to connect to the CN CALL Worker using DNS or verified IP fallbacks: $lastError',
    );
  }
}
