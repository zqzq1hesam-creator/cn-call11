import 'dart:io';

const String _cnCallWorkerHost =
    'cn-call11.zqzq1hesam-cncall.workers.dev';

// These Cloudflare anycast addresses are used only after normal DNS fails.
// The fallback still performs TLS with the original hostname, SNI and normal
// certificate validation; it never sends HTTP in cleartext to port 443.
const List<String> _cnCallWorkerIpv4Fallbacks = <String>[
  '172.67.136.179',
  '104.21.62.147',
];

/// Installs a DNS fallback for this Worker without bypassing HTTPS/TLS checks.
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
      // A configured proxy must receive a normal TCP connection. HttpClient
      // is responsible for establishing CONNECT/TLS when HTTPS uses a proxy.
      if (proxyHost != null && proxyPort != null) {
        return Socket.startConnect(proxyHost, proxyPort);
      }

      // IMPORTANT: connectionFactory must return a SecureSocket for a direct
      // HTTPS request. Returning Socket.startConnect here sends plain HTTP to
      // port 443 and produces: "The plain HTTP request was sent to HTTPS port".
      if (uri.scheme == 'https') {
        if (uri.host.toLowerCase() == _cnCallWorkerHost) {
          return Future<ConnectionTask<Socket>>.value(
            ConnectionTask.fromSocket<Socket>(
              _connectWorkerSecure(uri.port, context),
              () {},
            ),
          );
        }

        // Keep normal TLS for any other HTTPS destination too.
        return SecureSocket.startConnect(
          uri.host,
          uri.port,
          context: context,
        );
      }

      // Ordinary HTTP destinations use a plain socket on their own port.
      return Socket.startConnect(uri.host, uri.port);
    };
    return client;
  }

  Future<SecureSocket> _connectWorkerSecure(
    int port,
    SecurityContext? context,
  ) async {
    Object? lastError;

    // First attempt: standard DNS plus TLS, with certificate validation.
    try {
      return await SecureSocket.connect(
        _cnCallWorkerHost,
        port,
        context: context,
        timeout: const Duration(seconds: 3),
      );
    } catch (error) {
      lastError = error;
    }

    // DNS fallback: connect the TCP socket to a known Cloudflare IPv4 address,
    // then explicitly perform TLS using the real workers.dev hostname. This
    // preserves SNI and hostname verification.
    for (final address in _cnCallWorkerIpv4Fallbacks) {
      Socket? rawSocket;
      try {
        rawSocket = await Socket.connect(
          InternetAddress(address),
          port,
          timeout: const Duration(seconds: 4),
        );
        final socket = rawSocket;
        rawSocket = null; // SecureSocket owns the socket from this point.
        return await SecureSocket.secure(
          socket,
          host: _cnCallWorkerHost,
          context: context,
        ).timeout(const Duration(seconds: 6));
      } catch (error) {
        rawSocket?.destroy();
        lastError = error;
      }
    }

    throw SocketException(
      'Unable to establish HTTPS/TLS with the CN CALL Worker using DNS or '
      'verified IP fallbacks: $lastError',
    );
  }
}
