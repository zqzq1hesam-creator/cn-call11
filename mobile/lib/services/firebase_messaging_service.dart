import 'dart:async';
import 'dart:convert';

import 'package:firebase_core/firebase_core.dart';
import 'package:firebase_messaging/firebase_messaging.dart';
import 'package:http/http.dart' as http;

import 'call_session.dart';
import 'rtc_call_manager.dart';
import 'server_config.dart';

@pragma('vm:entry-point')
Future<void> firebaseMessagingBackgroundHandler(
  RemoteMessage message,
) async {
  print('FCM BACKGROUND MESSAGE: ${message.messageId}');

  await Firebase.initializeApp();

  final type = message.data['type']?.toString();

  if (type == 'incoming_call') {
    final callId = message.data['call_id']?.toString();
    if (callId == null ||
      callId.isEmpty ||
        await CallSession.instance.isCallEnded(callId) ||
        await CallSession.instance.hasActiveCall()) {
      return;
    }

    // Native CallFirebaseService owns incoming_call delivery through Telecom.
    // Do not persist a second Flutter pending call for the same call_id.
    print('FCM BACKGROUND: incoming_call owned by native Telecom call_id=$callId');
    return;
  }

  if (type == 'call_cancelled' || type == 'call_reject' || type == 'hangup' || type == 'timeout' || type == 'disconnected') {
    final callId = message.data['call_id']?.toString();
    await RtcCallManager.instance.handleRemoteTermination(
      callId: callId,
      reason: type == 'call_reject' ? 'rejected' : type == 'call_cancelled' ? 'cancelled' : 'ended',
    );

    print('FCM BACKGROUND: force-closed cancelled call');
  }
}

class FirebaseMessagingService {
  FirebaseMessagingService._();

  static final FirebaseMessagingService instance =
      FirebaseMessagingService._();

  final FirebaseMessaging _messaging =
      FirebaseMessaging.instance;

  StreamSubscription<String>? _tokenSubscription;
  StreamSubscription<RemoteMessage>? _messageSubscription;

  String? _token;

  String? get token => _token;

  Future<String?> initialize() async {
    try {
      final settings =
          await _messaging.requestPermission(
        alert: true,
        badge: true,
        sound: true,
        provisional: false,
      );

      print(
        'FCM notification permission: '
        '${settings.authorizationStatus}',
      );

      _token = await _messaging.getToken();

      if (_token != null && _token!.isNotEmpty) {
        await _sendTokenToServer(_token!);
      }

      await _tokenSubscription?.cancel();

      _tokenSubscription =
          _messaging.onTokenRefresh.listen(
        (token) async {
          _token = token;

          await _sendTokenToServer(token);
        },
      );

      await _messageSubscription?.cancel();

      _messageSubscription =
          FirebaseMessaging.onMessage.listen(
        (RemoteMessage message) async {
          print(
            'FCM FOREGROUND MESSAGE: '
            '${message.messageId}',
          );

          final foregroundType =
              message.data['type']?.toString();

          if (foregroundType == 'call_cancelled' || foregroundType == 'call_reject' || foregroundType == 'hangup' || foregroundType == 'timeout' || foregroundType == 'disconnected') {
            final callId = message.data['call_id']?.toString();
            await RtcCallManager.instance.handleRemoteTermination(
              callId: callId,
              reason: foregroundType == 'call_reject' ? 'rejected' : foregroundType == 'call_cancelled' ? 'cancelled' : 'ended',
            );

            return;
          }

          if (foregroundType != 'incoming_call') {
            return;
          }

          final callId = message.data['call_id']?.toString();
          if (callId == null || callId.isEmpty) return;

          if (await CallSession.instance.isCallEnded(callId)) return;
          if (await CallSession.instance.hasActiveCall()) return;
          // Native CallFirebaseService owns incoming_call delivery through
          // Telecom. Keeping this out of pending_incoming_call prevents the
          // legacy Flutter incoming screen from duplicating the Telecom call.
          print('FCM FOREGROUND: incoming_call owned by native Telecom call_id=$callId');
        },
      );

      return _token;
    } catch (e) {
      print('FCM initialization error: $e');
      return null;
    }
  }

  Future<void> _sendTokenToServer(
    String token,
  ) async {
    final userId = CallSession.instance.userId;
    final accessToken = CallSession.instance.accessToken;

    if (userId == null ||
      userId.isEmpty ||
      accessToken == null ||
      accessToken.isEmpty) {
      print(
        'FCM: user not logged in yet',
      );
      return;
    }

    try {
      final response = await http.post(
        Uri.parse(
          '${ServerConfig.httpUrl}/fcm-token',
        ),
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Bearer $accessToken',
        },
        body: jsonEncode({
          'token': token,
        }),
      );

      print(
        'FCM TOKEN SERVER RESPONSE: '
        '${response.statusCode}',
      );

      print(
        'FCM TOKEN SERVER BODY: '
        '${response.body}',
      );
    } catch (e) {
      print(
        'FCM TOKEN SERVER ERROR: $e',
      );
    }
  }

  Future<void> refreshTokenForCurrentUser() async {
    try {
      final token = await _messaging.getToken();

      if (token == null || token.isEmpty) {
        print('FCM REFRESH: no token');
        return;
      }

      _token = token;

      await _sendTokenToServer(token);
    } catch (e) {
      print(
        'FCM REFRESH ERROR: $e',
      );
    }
  }

  Future<void> dispose() async {
    await _tokenSubscription?.cancel();
    await _messageSubscription?.cancel();

    _tokenSubscription = null;
    _messageSubscription = null;
  }
}
