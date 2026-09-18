import 'dart:async';
import 'dart:convert';

import 'package:flutter/services.dart';
import 'package:shared_preferences/shared_preferences.dart';

import 'call_socket.dart';
import 'rtc_call_manager.dart';

class CallSession {
  static final CallSession instance = CallSession._();

  final CallSocket socket = CallSocket();
  Function()? onSessionInvalidated;

  CallSession._() {
    socket.onSessionInvalid = invalidateSession;
    socket.onCheckOwnership = guardFlutterWsOwnership;
  }

  StreamSubscription<Map<String, dynamic>>? _messageSubscription;

  String? userId;
  String? displayName;
  String? accessToken;

  bool get loggedIn => userId != null;

  // This is a JSON string rather than a StringList. Android native code must
  // read the exact same representation from FlutterSharedPreferences.
  static const _endedCallIdsKey = 'cn_call_ended_call_ids_v2';
  static const _activeCallIdKey = 'cn_call_active_call_id';
  static const _activeCallAtKey = 'cn_call_active_call_at';

  /// Phase 2: shared signaling-owner marker. Stored in FlutterSharedPreferences
  /// (native reads the fully-qualified "flutter.cn_call_ws_owner" key).
  static const _wsOwnerKey = 'cn_call_ws_owner';

  Future<bool> hasActiveCall() async {
    final prefs = await SharedPreferences.getInstance();
    final activeId = prefs.getString(_activeCallIdKey);

    // لا تنتهي حالة المكالمة تلقائيًا بعد 90 ثانية.
    // المكالمة تبقى نشطة حتى يتم استدعاء markCallEnded().
    // هذا يمنع استقبال/بدء مكالمة ثانية أثناء وجود مكالمة فعلية.
    return activeId != null && activeId.isNotEmpty;
  }

  Future<void> markCallActive(String callId) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString(_activeCallIdKey, callId);
    await prefs.setInt(_activeCallAtKey, DateTime.now().millisecondsSinceEpoch);
  }

  Future<bool> isCallEnded(String? callId) async {
    final id = callId?.trim() ?? '';
    if (id.isEmpty) return true;

    final prefs = await SharedPreferences.getInstance();
    return _readEndedCallIds(prefs).contains(id);
  }

  Future<void> markCallEnded(String? callId) async {
    final id = callId?.trim() ?? '';
    if (id.isEmpty) return;

    final prefs = await SharedPreferences.getInstance();
    final ids = _readEndedCallIds(prefs);
    ids.remove(id);
    ids.add(id);
    if (ids.length > 32) ids.removeRange(0, ids.length - 32);
    await prefs.setString(_endedCallIdsKey, jsonEncode(ids));
    if (prefs.getString(_activeCallIdKey) == id) {
      await prefs.remove(_activeCallIdKey);
      await prefs.remove(_activeCallAtKey);
    }
  }

  List<String> _readEndedCallIds(SharedPreferences prefs) {
    final encoded = prefs.getString(_endedCallIdsKey);
    if (encoded == null || encoded.isEmpty) return <String>[];
    try {
      final decoded = jsonDecode(encoded);
      if (decoded is List) {
        return decoded
            .map((value) => value.toString().trim())
            .where((value) => value.isNotEmpty)
            .toList();
      }
    } catch (_) {
      // A malformed tombstone must never crash an incoming-call handler.
    }
    return <String>[];
  }

  Future<void> login({
    required String id,
    required String name,
    required String token,
  }) async {
    userId = id;
    displayName = name;
    accessToken = token;

    final prefs = await SharedPreferences.getInstance();
    await prefs.setString('cn_call_user_id', id);
    await prefs.setString('cn_call_display_name', name);
    await prefs.setString('cn_call_access_token', token);

    try {
      await const MethodChannel('cn_call/call').invokeMethod('syncCredentials');
    } catch (e) {
      print('[CN CALL][SESSION] syncCredentials method channel error: $e');
    }
  }

  /// Ownership guard. Two callers:
  ///
  /// 1. Installed on [CallSocket] ([onCheckOwnership]) so connect/reconnect is
  ///    allowed only while Flutter still owns signaling.
  /// 2. Called live by [RtcCallManager] immediately BEFORE a `call_accept`
  ///    is sent (Phase 5): a native-managed call may have become the WS owner
  ///    since the socket connected, and the accept must then be withheld —
  ///    Flutter never sends `call_accept` and never tries to seize ownership.
  ///
  /// Returns true only when no other owner (currently "native", set by a
  /// native-managed call) holds the socket. This is the single place Flutter
  /// writes the owner marker.
  Future<bool> guardFlutterWsOwnership() async {
    final prefs = await SharedPreferences.getInstance();
    final current = prefs.getString(_wsOwnerKey) ?? '';
    if (current == 'native') {
      return false;
    }
    if (current != 'flutter') {
      await prefs.setString(_wsOwnerKey, 'flutter');
    }
    return true;
  }

  /// Phase 2: clears the Flutter owner marker (used on logout and session
  /// invalidation so a later native-managed call can acquire ownership).
  Future<void> releaseWsOwnership() async {
    final prefs = await SharedPreferences.getInstance();
    if (prefs.getString(_wsOwnerKey) == 'flutter') {
      await prefs.remove(_wsOwnerKey);
    }
  }

  /// Cold-start staleness repair. A freshly started Flutter isolate can hold
  /// no live Flutter-managed call, so any lingering `flutter` WS-ownership
  /// marker and any leftover active-call marker came from a PREVIOUS process
  /// (crash / OS kill mid-call). Clear them BEFORE the socket reconnects so:
  ///   - the next native acquisition is never refused
  ///     (flutterHasManagedCall / owner=="flutter" in NativeWebSocketClient),
  ///   - the Flutter socket stays eligible to reclaim ownership on reconnect.
  /// Only the `flutter` marker is removed when present; a `native` marker
  /// (an active native/managed call in flight from this same process) is left
  /// untouched, and the active-call id is cleared only while this fresh
  /// isolate is not itself managing a call.
  Future<void> clearStaleStateForFreshStartup() async {
    final prefs = await SharedPreferences.getInstance();

    final active = prefs.getString(_activeCallIdKey);
    if (active != null && active.isNotEmpty) {
      await prefs.remove(_activeCallIdKey);
      await prefs.remove(_activeCallAtKey);
    }

    await releaseWsOwnership();
  }

  Future<void> invalidateSession() async {
    try {
      await const MethodChannel('cn_call/call').invokeMethod('logout');
    } catch (e) {
      print('[CN CALL][SESSION] logout method channel error: $e');
    }
    await RtcCallManager.instance.endForSession(sendSignal: false);
    socket.disconnect();
    await releaseWsOwnership();
    userId = null;
    displayName = null;
    accessToken = null;

    final prefs = await SharedPreferences.getInstance();
    await prefs.remove('cn_call_user_id');
    await prefs.remove('cn_call_display_name');
    await prefs.remove('cn_call_access_token');

    onSessionInvalidated?.call();
  }

  Future<bool> restoreSession() async {
    final prefs = await SharedPreferences.getInstance();

    final id = prefs.getString('cn_call_user_id');
    final name = prefs.getString('cn_call_display_name');
    final token = prefs.getString('cn_call_access_token');

    if (id == null ||
        id.isEmpty ||
        name == null ||
        name.isEmpty ||
        token == null ||
        token.isEmpty) {
      return false;
    }

    userId = id;
    displayName = name;
    accessToken = token;

    try {
      await const MethodChannel('cn_call/call').invokeMethod('syncCredentials');
    } catch (e) {
      print('[CN CALL][SESSION] syncCredentials method channel error: $e');
    }

    return true;
  }

  Future<bool> restoreCredentialsOnly() async {
    final prefs = await SharedPreferences.getInstance();

    final id = prefs.getString('cn_call_user_id')?.trim();
    final name = prefs.getString('cn_call_display_name')?.trim();
    final token = prefs.getString('cn_call_access_token')?.trim();

    if (id == null ||
        id.isEmpty ||
        name == null ||
        name.isEmpty ||
        token == null ||
        token.isEmpty) {
      return false;
    }

    userId = id;
    displayName = name;
    accessToken = token;
    return true;
  }

  /// Restores the authenticated WebSocket before a Telecom background action
  /// emits any signalling. This is shared by accepted incoming and outgoing.
  Future<void> ensureSocketReady() async {
    final id = userId;
    final token = accessToken;
    if (id == null || id.isEmpty || token == null || token.isEmpty) {
      throw StateError('No authenticated CN CALL session');
    }
    try {
      await const MethodChannel('cn_call/call').invokeMethod('syncCredentials');
    } catch (_) {}
  }


  Future<bool> logout() async {
    try {
      final hasActive = await const MethodChannel('cn_call/call').invokeMethod<bool>('hasActiveCall') ?? false;
      if (hasActive) {
        print('[CN CALL][SESSION] logout refused: native Telecom call is active');
        return false;
      }
      await const MethodChannel('cn_call/call').invokeMethod('logout');
    } catch (e) {
      print('[CN CALL][SESSION] logout method channel error: $e');
    }

    await _messageSubscription?.cancel();
    _messageSubscription = null;
    await RtcCallManager.instance.endForSession(sendSignal: false);
    socket.disconnect();
    await releaseWsOwnership();

    userId = null;
    displayName = null;
    accessToken = null;

    final prefs = await SharedPreferences.getInstance();
    await prefs.remove('cn_call_user_id');
    await prefs.remove('cn_call_display_name');
    await prefs.remove('cn_call_access_token');
    return true;
  }

  Future<void> dispose() async {
    socket.disconnect();
  }
}
