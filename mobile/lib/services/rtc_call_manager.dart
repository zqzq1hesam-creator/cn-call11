// ignore_for_file: avoid_print

import 'dart:developer' as developer;
import 'package:shared_preferences/shared_preferences.dart';
import 'dart:async';
import 'package:flutter/services.dart';
import 'package:audioplayers/audioplayers.dart';

import 'package:uuid/uuid.dart';

import 'call_session.dart';
import 'call_coordinator.dart';
import 'livekit_call.dart';
import 'livekit_token_service.dart';

enum CallState {
  incoming,
  ringing,
  accepted,
  negotiating,
  rejected,
  cancelled,
  connecting,
  connected,
  ended,
  timeout,
  offline,
}

class RtcCallManager {
  RtcCallManager._();

  static final RtcCallManager instance = RtcCallManager._();

  final CallSession session = CallSession.instance;
  final LiveKitCall livekit = LiveKitCall();
  final AudioPlayer _ringPlayer = AudioPlayer();

  Timer? _ringTimeoutTimer;
  Timer? _negotiationTimeoutTimer;
  Timer? _connectionTimeoutTimer;

  StreamSubscription<Map<String, dynamic>>? _subscription;

  String? remoteUserId;
  String? currentCallId;
  bool? remoteOnline;
  CallState? state;
  bool inCall = false;
  bool caller = false;
  bool _muted = false;

  final List<Map<String, dynamic>> _pendingIceCandidates = [];

  Function()? onConnected;
  Function()? onDisconnected;
  Function(String callId)? onRemoteCallCancelled;
  Function(bool online)? onRemoteAvailabilityChanged;

  bool _started = false;
  bool _hangingUp = false;
  Future<void>? _cleanupFuture;
  bool _ringbackPlaying = false;
  bool _incomingRingtonePlaying = false;
  String? _nativeOutgoingRingbackCallId;
  int _nativeOutgoingRingbackGeneration = 0;
  bool _nativeOutgoingCallActive = false;
  Completer<bool>? _callStartCompleter;
  int? _callStartExpiresAt;

  Future<void> _startRinging({
    required String callId,
    int? expiresAt,
  }) async {
    _ringTimeoutTimer?.cancel();

    Duration duration = const Duration(seconds: 90);

    if (expiresAt != null) {
      final remainingMs = expiresAt - DateTime.now().millisecondsSinceEpoch;

      if (remainingMs <= 0) {
        await _handleRingTimeout(callId);
        return;
      }

      final cappedMs = remainingMs > 90000 ? 90000 : remainingMs;
      duration = Duration(milliseconds: cappedMs);
    }

    _ringTimeoutTimer = Timer(
      duration,
      () => _handleRingTimeout(callId),
    );

    try {
      await _ringPlayer.setReleaseMode(ReleaseMode.loop);

      await _ringPlayer.setAudioContext(
        AudioContext(
          android: AudioContextAndroid(
            usageType: AndroidUsageType.voiceCommunication,
            contentType: AndroidContentType.speech,
            audioFocus: AndroidAudioFocus.gainTransient,
          ),
        ),
      );

      await _ringPlayer.play(
        AssetSource('sounds/ringing.mp3'),
      );
      _ringbackPlaying = true;

      print(
        '[CN CALL][RINGBACK START] call_id=$callId route=earpiece default_ringtone',
      );
    } catch (e) {
      _ringbackPlaying = false;

      print(
        '[CN CALL][RINGBACK FAILED] call_id=$callId error=$e',
      );
    }
  }

  Future<void> startOutgoingRingback(String callId) async {
    developer.log(
      'callId=$callId '
      'currentCallId=$currentCallId '
      'caller=$caller '
      'inCall=$inCall '
      'active=$_nativeOutgoingCallActive '
      'registered=$_nativeOutgoingRingbackCallId '
      'generation=$_nativeOutgoingRingbackGeneration',
      name: 'CNCall.ringback',
    );
    final generation = _nativeOutgoingRingbackGeneration;
    if (!caller ||
        currentCallId != callId ||
        _nativeOutgoingRingbackCallId != callId ||
        generation != _nativeOutgoingRingbackGeneration ||
        _nativeOutgoingCallActive ||
        inCall) {
      return;
    }

    await _startRinging(callId: callId);
  }

  Future<void> prepareNativeOutgoingCall(String callId) async {
    _nativeOutgoingRingbackCallId = callId;
    _nativeOutgoingRingbackGeneration++;
    _nativeOutgoingCallActive = false;
    currentCallId = callId;
    caller = true;
    inCall = false;
    state = CallState.ringing;
  }

  Future<void> abortNativeOutgoingCall(String callId) async {
    _nativeOutgoingRingbackGeneration++;
    _nativeOutgoingRingbackCallId = null;
    _nativeOutgoingCallActive = false;
    await _stopRinging();
    if (currentCallId != callId) return;
    currentCallId = null;
    remoteUserId = null;
    caller = false;
    inCall = false;
    state = null;
  }

  Future<void> _stopRinging() async {
    _ringTimeoutTimer?.cancel();
    _ringTimeoutTimer = null;

    if (_ringbackPlaying || _incomingRingtonePlaying) {
      try {
        await const MethodChannel('cn_call/call').invokeMethod(
          'stopDefaultRingtone',
        );
      } catch (e) {
        print('[CN CALL][RINGBACK STOP FAILED] error=$e');
      }

      await _ringPlayer.stop();
      _ringbackPlaying = false;
      _incomingRingtonePlaying = false;
      print('[CN CALL][RINGBACK STOP]');
    }
  }

  void _cancelCallTimeouts() {
    _ringTimeoutTimer?.cancel();
    _ringTimeoutTimer = null;
    _negotiationTimeoutTimer?.cancel();
    _negotiationTimeoutTimer = null;
    _connectionTimeoutTimer?.cancel();
    _connectionTimeoutTimer = null;
  }

  void _startNegotiationTimeout(String callId) {
    _negotiationTimeoutTimer?.cancel();
    _negotiationTimeoutTimer = Timer(
      const Duration(seconds: 30),
      () => _handleNegotiationTimeout(callId),
    );
  }

  void _startConnectionTimeout(String callId) {
    _connectionTimeoutTimer?.cancel();
    _connectionTimeoutTimer = Timer(
      const Duration(seconds: 30),
      () => _handleConnectionTimeout(callId),
    );
  }

  Future<void> _handleNegotiationTimeout(String callId) async {
    if (!_isCurrentCall(callId) ||
        state == CallState.connected ||
        state == CallState.ended) {
      return;
    }

    print('[CN CALL][TIMEOUT] negotiation call_id=$callId');
    await _cleanupCall(
      reason: 'timeout',
      sendSignal: true,
      signalType: 'hangup',
      forceDisconnect: true,
    );
  }

  Future<void> _handleConnectionTimeout(String callId) async {
    if (!_isCurrentCall(callId) ||
        state == CallState.connected ||
        state == CallState.ended) {
      return;
    }

    print('[CN CALL][TIMEOUT] connection call_id=$callId');
    await _cleanupCall(
      reason: 'timeout',
      sendSignal: true,
      signalType: 'hangup',
      forceDisconnect: true,
    );
  }

  Future<void> _handleRingTimeout(String callId) async {
    if (!_isCurrentCall(callId) || !caller || inCall) return;

    print('[CN CALL][RING] timeout reached');

    await _cleanupCall(
      reason: 'timeout',
      sendSignal: true,
      signalType: 'call_cancelled',
      forceDisconnect: true,
    );
  }

  void startListening() {
    if (_started) return;
    _started = true;

    _subscription = session.socket.messages.listen(_handleMessage);

    livekit.onConnected = () {
      final connectedCallId = currentCallId;
      final target = remoteUserId;

      if (_hangingUp || connectedCallId == null || connectedCallId.isEmpty) {
        return;
      }

      _cancelCallTimeouts();
      inCall = true;
      state = CallState.connected;

      print(
        '[CN CALL][LIVEKIT MANAGER] connected '
        'call_id=$connectedCallId',
      );

      onConnected?.call();
    };

    livekit.onDisconnected = () {
      if (_hangingUp) return;

      print('[CN CALL][LIVEKIT CONNECT FAILED] call_id=$currentCallId disconnected');

      unawaited(
        _cleanupCall(reason: 'failed', sendSignal: true, signalType: 'hangup'),
      );
    };
  }

  Future<void> _handleMessage(Map<String, dynamic> message) async {
    final type = message['type']?.toString();
    final messageCallId = message['call_id']?.toString().trim();

    if (type == 'call') {
      print(
          '[CN CALL][CALL RECEIVE] Suppressed Flutter incoming; Native Telecom owns call_id=$messageCallId');
      return;
    }

    if (type == 'call_started') {
      if (!_isCurrentCall(messageCallId)) return;

      remoteOnline = message['target_online'] == true;
      onRemoteAvailabilityChanged?.call(remoteOnline!);

      // Offline does NOT mean the call failed.
      // The server has already created the call and sent FCM.
      // Keep the caller ringing until the server-provided 90s expiry.
      state = CallState.ringing;
      print('[CN CALL][CALL UI RINGING] call_id=$messageCallId');

      final expiresAtRaw = message['ring_expires_at'];
      _callStartExpiresAt = expiresAtRaw is int
          ? expiresAtRaw
          : int.tryParse(expiresAtRaw?.toString() ?? '');

      // call_started itself confirms that the server accepted the call.
      // target_online only tells us whether the target has a live WebSocket.
      _callStartCompleter?.complete(true);
      _callStartCompleter = null;

      return;
    }

    if (type == 'call_accept') {
      if (!_isCurrentCall(messageCallId)) return;
      print('[CN CALL][CALL ACCEPT FORWARD] call_id=$messageCallId');
      await _handleAccepted();
      return;
    }

    if (type == 'call_cancelled') {
      await handleRemoteTermination(callId: messageCallId, reason: 'cancelled');
      return;
    }

    if (type == 'hangup' || type == 'call_reject') {
      await handleRemoteTermination(
        callId: messageCallId,
        reason: type == 'call_reject' ? 'rejected' : 'ended',
      );
      return;
    }
  }

  /// Handles a terminal event from WebSocket or FCM exactly once at the call
  /// state boundary.  Persisting the tombstone before touching native UI makes
  /// late `incoming_call` pushes, reconnects and pending-call restoration
  /// harmless.
  Future<void> handleRemoteTermination({
    required String? callId,
    required String reason,
  }) async {
    final id = callId?.trim() ?? '';
    if (id.isEmpty) return;

    await session.markCallEnded(id);

    if (reason == 'cancelled') {
      onRemoteCallCancelled?.call(id);
    }

    if (!_isCurrentCall(id)) return;

    await _cleanupCall(
      reason: reason,
      forceDisconnect: true,
    );
  }

  bool _isCurrentCall(String? callId) {
    return callId != null && callId.isNotEmpty && callId == currentCallId;
  }

  Future<void> _handleAccepted() async {
    if (!caller) return;
    if (state == CallState.connecting || state == CallState.connected) return;

    await _stopRinging();

    final acceptedCallId = currentCallId;
    if (acceptedCallId == null || acceptedCallId.isEmpty) return;

    state = CallState.negotiating;

    try {
      await _connectLiveKit(acceptedCallId);
    } catch (e) {
      print('[CN CALL][LIVEKIT CONNECT FAILED] call_id=$acceptedCallId error=$e');
      await _failTelecomAndCleanup(acceptedCallId, reason: 'failed');
    }
  }

  Future<void> hangup({bool sendSignal = true}) async {
    final shouldCancel = caller && !inCall;
    await _cleanupCall(
      reason: shouldCancel ? 'cancelled' : 'ended',
      sendSignal: sendSignal,
      signalType: shouldCancel ? 'call_cancelled' : 'hangup',
      forceDisconnect: true,
    );
  }

  Future<void> endForSession({bool sendSignal = true}) {
    return _cleanupCall(
      reason: 'ended',
      sendSignal: sendSignal,
      signalType: 'hangup',
      forceDisconnect: true,
    );
  }

  /// Native-owned outgoing call reached ACTIVE (media ready). Keeps the
  /// in-app CallScreen bound to the same native call without any Flutter
  /// LiveKit/WebSocket of its own for that callId.
  Future<void> onNativeCallActive() async {
    final callId = currentCallId;
    if (callId == null || callId.isEmpty) return;
    _nativeOutgoingRingbackGeneration++;
    _nativeOutgoingRingbackCallId = null;
    _nativeOutgoingCallActive = true;
    await _stopRinging();
    _cancelCallTimeouts();
    inCall = true;
    state = CallState.connected;
    print('[CN CALL][NATIVE ACTIVE] call_id=$callId');
    onConnected?.call();
  }

  /// Native-owned call ended (event pushed by CNCallConnection after Telecom
  /// reached setDisconnected). The terminal frame was already sent by the
  /// native engine; Flutter only mirrors local state — one terminal path.
  Future<void> handleNativeCallEnded(String callId) async {
    if (!_isCurrentCall(callId)) return;
    print('[CN CALL][NATIVE ENDED] call_id=$callId');
    await _cleanupCall(
      reason: 'ended',
      sendSignal: false,
      forceDisconnect: true,
    );
  }

  /// App-originated outgoing: end the SAME native Telecom call (the in-app
  /// CallScreen maps to the system connection created by placeCNCall). Never
  /// sends a Flutter WebSocket terminal frame for it — that is the native
  /// engine's single responsibility.
  Future<void> endActiveNativeTelecomCall() async {
    final callId = currentCallId;
    if (callId != null && callId.isNotEmpty) {
      try {
        await const MethodChannel('cn_call/call').invokeMethod(
          'endActiveTelecomCall',
          <String, dynamic>{'callId': callId},
        );
      } catch (error) {
        print('[CN CALL][NATIVE END FAILED] call_id=$callId error=$error');
      }
    }
    await _cleanupCall(
      reason: 'ended',
      sendSignal: false,
      forceDisconnect: true,
    );
  }

  Future<void> _cleanupCall({
    required String reason,
    bool sendSignal = false,
    String? signalType,
    bool forceDisconnect = false,
  }) async {
    if (_hangingUp) return _cleanupFuture ?? Future<void>.value();

    _hangingUp = true;
    final callId = currentCallId;
    final target = remoteUserId;

    final cleanup = () async {
      print('[CN CALL][CALL CLEANUP START] call_id=$callId reason=$reason force=$forceDisconnect');
      try {
      _callStartCompleter?.complete(false);
      _callStartCompleter = null;
      _callStartExpiresAt = null;
      _cancelCallTimeouts();
      _nativeOutgoingRingbackGeneration++;
      _nativeOutgoingRingbackCallId = null;
      _nativeOutgoingCallActive = false;
      await _stopRinging();

      if (sendSignal && callId != null && target != null && session.loggedIn) {
        print('[CN CALL][CALL TERMINAL] native owns terminal signaling call_id=$callId type=${signalType ?? 'hangup'}');
      }

      await livekit.disconnect();

      await session.markCallEnded(callId);
      if (callId != null) CallCoordinator.instance.markEnded(callId);

      _pendingIceCandidates.clear();
      remoteUserId = null;
      currentCallId = null;
      remoteOnline = null;
      inCall = false;
      caller = false;
      _muted = false;
      state = switch (reason) {
        'cancelled' => CallState.cancelled,
        'rejected' => CallState.rejected,
        'timeout' => CallState.timeout,
        'failed' => CallState.ended,
        _ => CallState.ended,
      };

      if (callId != null) onDisconnected?.call();
      print('[CN CALL][CALL CLEANUP DONE] call_id=$callId reason=$reason');
      } finally {
        _hangingUp = false;
        _cleanupFuture = null;
      }
    }();
    _cleanupFuture = cleanup;
    return cleanup;
  }

  Future<void> mute(bool value) async {
    _muted = value;
    await livekit.mute(value);
  }


  Future<void> setSpeaker(bool value) {
    return livekit.setSpeaker(value);
  }

  Future<void> dispose() async {
    await _cleanupCall(reason: 'ended', sendSignal: true);
    await _subscription?.cancel();
    _subscription = null;
    _started = false;

    await livekit.disconnect();
  }

  Future<void> _connectLiveKit(String callId) async {
    print('[CN CALL][LIVEKIT START] call_id=$callId');

    final data = await LiveKitTokenService.getToken(callId: callId);

    final url = data['url']?.toString();
    final token = data['token']?.toString();

    if (url == null || url.isEmpty) {
      throw Exception('LiveKit response missing url');
    }

    if (token == null || token.isEmpty) {
      throw Exception('LiveKit response missing token');
    }

    await livekit.connect(url: url, token: token);
    print('[CN CALL][LIVEKIT CONNECTED] call_id=$callId');

    // Preserve a mute choice made on the incoming CN CALL screen while the
    // LiveKit room was still connecting.
    if (_muted) await livekit.mute(true);

    if (!_isCurrentCall(callId)) {
      await livekit.disconnect();
      throw StateError('LiveKit connected for a stale call');
    }

    livekit.notifyConnected();
  }


  Future<void> _failTelecomAndCleanup(String callId, {required String reason}) async {
    await _cleanupCall(
      reason: reason,
      sendSignal: true,
      signalType: 'hangup',
      forceDisconnect: true,
    );
  }
}
