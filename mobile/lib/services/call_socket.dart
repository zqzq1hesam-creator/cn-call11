import 'dart:async';
import 'dart:convert';

import 'package:web_socket_channel/web_socket_channel.dart';

import 'server_config.dart';

class CallSocket {
  WebSocketChannel? _channel;
  String? _userId;
  String? _token;
  Timer? _reconnectTimer;
  bool _connecting = false;
  Completer<void>? _readyCompleter;
  bool _reconnectEnabled = true;
  int _reconnectAttempt = 0;
  int _connectionGeneration = 0;
  bool _ready = false;
  Future<void> Function()? onSessionInvalid;

  /// Phase 2: returns true only while Flutter may hold the signaling socket.
  /// Consulted before every connect/reconnect; when it returns false the
  /// socket must not be opened or reopened.
  Future<bool> Function()? onCheckOwnership;

  /// Phase 2: serializes the async ownership read in [_scheduleReconnect] so
  /// two overlapping calls can never create two reconnect timers.
  bool _reconnectScheduling = false;

  /// True only after the WebSocket handshake completed.  A non-null channel
  /// merely means that a connection attempt was created; it cannot carry a
  /// call control message yet.
  bool get connected => _channel != null && _ready;

  final StreamController<Map<String, dynamic>> _messages =
      StreamController<Map<String, dynamic>>.broadcast();

  Stream<Map<String, dynamic>> get messages => _messages.stream;

  Future<void> connect(String userId, String token) async {
    if (connected && _userId == userId && _token == token) return;
    if (_connecting) return _readyCompleter?.future ?? Future<void>.value();

    // Phase 2.1 ownership gate: never open (or reopen) a WebSocket when
    // Flutter does not currently own signaling. Ownership loss is terminal for
    // this socket: close and stop reconnect (credentials are kept so a later
    // explicit connect() can re-take ownership once it is free).
    if (!await _mayOwnSocket()) {
      print('SOCKET CONNECT SKIPPED: Flutter is not the WS owner');
      _disableReconnect();
      return;
    }

    _userId = userId;
    _token = token;
    _reconnectEnabled = true;
    _reconnectScheduling = false;

    _connecting = true;
    _readyCompleter = Completer<void>();
    final generation = ++_connectionGeneration;

    final channel = WebSocketChannel.connect(
      Uri.parse('${ServerConfig.websocketUrl(userId)}?token=$token'),
    );

    try {
      await channel.ready;

      // logout/reconnect may have superseded this asynchronous connection
      // attempt while it was waiting for `ready`.
      final superseded = generation != _connectionGeneration ||
          !_reconnectEnabled ||
          _userId != userId ||
          _token != token;

      if (superseded) {
        await channel.sink.close();
        if (identical(_channel, channel)) {
          _channel = null;
          _ready = false;
        }
        if (generation == _connectionGeneration) {
          _connecting = false;
          _readyCompleter?.completeError(StateError('CN CALL socket superseded'));
          _readyCompleter = null;
        }
        return;
      }

      // Phase 2.1: ownership may have been lost while the handshake was in
      // flight (a native-managed call requested a handoff). Close this socket
      // and stop reconnect permanently — the loser never re-opens a socket.
      if (!await _mayOwnSocket()) {
        print('SOCKET CONNECT INTERRUPTED: Flutter lost WS ownership');
        await channel.sink.close();
        if (identical(_channel, channel)) {
          _channel = null;
          _ready = false;
        }
        _disableReconnect();
        return;
      }

      _channel = channel;
      _ready = true;
      _connecting = false;
      _reconnectAttempt = 0;
      _readyCompleter?.complete();
      _readyCompleter = null;

      print('SOCKET CONNECTED: ${ServerConfig.websocketUrl(userId)}');
      _flushPendingMessages();

      channel.stream.listen(
        (message) {
          try {
            final data = jsonDecode(message);

            if (data is Map) {
              final parsed = Map<String, dynamic>.from(data);

              if (parsed['type'] == 'session_invalid') {
                _handleSessionInvalid();
                return;
              }

              print(
                'SOCKET RECEIVE: type=${parsed['type']} '
                'call_id=${parsed['call_id']}',
              );

              _messages.add(parsed);
            }
          } catch (e) {
            print('SOCKET JSON ERROR: $e');
          }
        },
        onDone: () {
          print('SOCKET CLOSED');

          if (identical(_channel, channel)) {
            _channel = null;
            _ready = false;
            if (_reconnectEnabled) unawaited(_scheduleReconnect());
          }
        },
        onError: (error, stackTrace) {
          print('SOCKET ERROR: $error');

          if (identical(_channel, channel)) {
            _channel = null;
            _ready = false;
            if (_reconnectEnabled) unawaited(_scheduleReconnect());
          }
        },
        cancelOnError: false,
      );
    } catch (e) {
      print('SOCKET CONNECT ERROR: $e');

      await channel.sink.close();

      if (identical(_channel, channel)) {
        _channel = null;
        _ready = false;
      }

      if (generation == _connectionGeneration) {
        _connecting = false;
        _readyCompleter?.completeError(e);
        _readyCompleter = null;
        if (_reconnectEnabled) unawaited(_scheduleReconnect());
      }

      rethrow;
    }
  }

  /// Phase 2: consults the ownership guard when one is installed. A null guard
  /// means ownership decisions are not enforced (legacy behavior).
  Future<bool> _mayOwnSocket() {
    final check = onCheckOwnership;
    if (check == null) return Future<bool>.value(true);
    return check();
  }

  Future<void> _scheduleReconnect() async {
    final userId = _userId;
    final token = _token;
    if (!_reconnectEnabled ||
        userId == null ||
        userId.isEmpty ||
        token == null ||
        token.isEmpty ||
        _reconnectScheduling ||
        _reconnectTimer != null) {
      return;
    }

    _reconnectScheduling = true;
    try {
      // Phase 2: reconnect only while Flutter still owns signaling. If
      // ownership was lost, disable reconnect entirely so the loser never
      // re-opens a socket and evicts the current owner.
      if (!await _mayOwnSocket()) {
        print('SOCKET RECONNECT SKIPPED: Flutter lost WS ownership');
        _reconnectScheduling = false;
        _disableReconnect();
        return;
      }
    } catch (_) {
      _reconnectScheduling = false;
      return;
    }
    _reconnectScheduling = false;

    final delaySeconds = 1 << (_reconnectAttempt.clamp(0, 5));
    _reconnectAttempt++;

    _reconnectTimer = Timer(Duration(seconds: delaySeconds), () async {
      _reconnectTimer = null;
      try {
        await connect(userId, token);
      } catch (_) {
        unawaited(_scheduleReconnect());
      }
    });
  }

  void _flushPendingMessages() {
    // Control messages must not be replayed after reconnect.
  }
  Future<void> sendGuaranteed(Map<String, dynamic> data) async {
    final channel = _channel;

    print('SOCKET SEND: type=${data['type']} call_id=${data['call_id']}');

    if (channel == null || !_ready) {
      throw StateError('CN CALL socket is not ready');
    }
    channel.sink.add(jsonEncode(data));
  }

  /// Non-critical callers may deliberately use best-effort signalling.
  void send(Map<String, dynamic> data) {
    sendGuaranteed(data).catchError((Object error) {
      print('SOCKET SEND FAILED: $error');
    });
  }

  void disconnect() {
    _connectionGeneration++;
    _connecting = false;
    _readyCompleter?.complete();
    _readyCompleter = null;
    _reconnectEnabled = false;
    _reconnectScheduling = false;
    _userId = null;
    _token = null;
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
    _channel?.sink.close();
    _channel = null;
    _ready = false;
  }

  /// Phase 2.1: terminal stop for ownership loss. Closes the channel and
  /// disables reconnect, but keeps [_userId]/[_token] so a later explicit
  /// connect() can re-establish Flutter ownership once the marker is freed
  /// (at native call teardown) without requiring a full re-login.
  void _disableReconnect() {
    _connectionGeneration++;
    _connecting = false;
    _readyCompleter?.complete();
    _readyCompleter = null;
    _reconnectEnabled = false;
    _reconnectScheduling = false;
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
    final channel = _channel;
    _channel = null;
    _ready = false;
    channel?.sink.close();
  }

  Future<void> dispose() async {
    _connectionGeneration++;
    _connecting = false;
    _reconnectEnabled = false;
    await _messages.close();

    _userId = null;
    _token = null;
    _reconnectTimer?.cancel();
    _channel?.sink.close();
    _channel = null;
    _ready = false;
  }

  void _handleSessionInvalid() {
    _connectionGeneration++;
    _reconnectEnabled = false;
    _reconnectScheduling = false;
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
    _connecting = false;
    final channel = _channel;
    _channel = null;
    _ready = false;
    channel?.sink.close();
    onSessionInvalid?.call();
  }
}
