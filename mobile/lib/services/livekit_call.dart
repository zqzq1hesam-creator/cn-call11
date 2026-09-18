// ignore_for_file: avoid_print

import 'package:flutter_webrtc/flutter_webrtc.dart';
import 'package:flutter/services.dart';
import 'package:livekit_client/livekit_client.dart';

class LiveKitCall {
  Room? _room;

  Function()? onConnected;
  Function()? onDisconnected;

  Room? get room => _room;

  Future<void> connect({
    required String url,
    required String token,
  }) async {
    // Keep LiveKit's normal audio-session handling. CN CALL owns the call
    // state and controls; Android Telecom is not part of this media path.

    _room = Room(
      roomOptions: const RoomOptions(
        adaptiveStream: false,
        dynacast: false,
      ),
    );

    _room!.events.listen((event) async {
      if (event is RoomConnectedEvent) {
        print('[CN CALL][LIVEKIT] CONNECTED');
      }

      if (event is RoomDisconnectedEvent) {
        print('[CN CALL][LIVEKIT] DISCONNECTED');
        onDisconnected?.call();
      }

      if (event is TrackSubscribedEvent) {
        print(
          '[CN CALL][LIVEKIT] TRACK SUBSCRIBED: ${event.track.kind}',
        );

        if (event.track is RemoteAudioTrack) {
          final audioTrack = event.track as RemoteAudioTrack;
          await audioTrack.start();
          print('[CN CALL][LIVEKIT] REMOTE AUDIO STARTED');
        }
      }
    });

    print('[CN CALL][LIVEKIT CONNECT START]');

    // Configure Android's communication mode and audio focus explicitly.
    // CN CALL owns routing: earpiece is default, speaker is opt-in.
    const callChannel = MethodChannel('cn_call/call');
    try {
      await callChannel.invokeMethod(
        'configureCallAudio',
        <String, dynamic>{'speaker': false},
      );
    } catch (error) {
      print('[CN CALL][AUDIO] native focus setup unavailable: $error');
    }
    await Helper.setAndroidAudioConfiguration(
      AndroidAudioConfiguration.communication,
    );
    await AudioManager.instance.setSpeakerOutputPreferred(false);

    await _room!.connect(url, token);

    print('[CN CALL][LIVEKIT CONNECTED] localParticipant=${_room!.localParticipant != null}');

    await _enableAndVerifyMicrophone();
  }

  /// Called only after Telecom has accepted the already-verified media call.
  void notifyConnected() => onConnected?.call();

  Future<void> setSpeaker(bool value) async {
    try {
      await const MethodChannel('cn_call/call').invokeMethod(
        'configureCallAudio',
        <String, dynamic>{'speaker': value},
      );
    } catch (error) {
      print('[CN CALL][AUDIO] native route setup unavailable: $error');
    }
    await AudioManager.instance.setSpeakerOutputPreferred(value);
    print('[CN CALL][LIVEKIT] speaker=${value ? 'on' : 'off'}');
  }

  Future<void> mute(bool value) async {
    final participant = _room?.localParticipant;
    if (participant == null) return;

    await participant.setMicrophoneEnabled(!value);

    print(
      '[CN CALL][LIVEKIT] microphone '
      '${value ? 'muted' : 'unmuted'}',
    );
  }

  Future<void> disconnect() async {
    await _room?.disconnect();
    await _room?.dispose();
    _room = null;

    print('[CN CALL][LIVEKIT] DISCONNECTED');
  }

  Future<void> _enableAndVerifyMicrophone() async {

    try {
      final stream = await navigator.mediaDevices.getUserMedia({
        'audio': true,
        'video': false,
      });

      print(
        '[CN CALL][MIC PERMISSION] granted '
        'tracks=${stream.getAudioTracks().length}',
      );

      for (final track in stream.getAudioTracks()) {
        await track.stop();
      }
    } catch (e) {
      print('[CN CALL][MIC PERMISSION FAILED] error=$e');
      throw StateError('Microphone permission denied');
    }

    final participant = _room?.localParticipant;
    if (participant == null) {
      throw StateError('LiveKit local participant is unavailable');
    }
    print('[CN CALL][LOCAL PARTICIPANT READY]');
    // Cold-started calls can connect before WebRTC has activated capture. A
    // second enable after one event-loop turn turns this into an explicit
    // publication check instead of assuming Telecom mute is the cause.
    for (var attempt = 0; attempt < 2; attempt++) {
      print('[CN CALL][MIC ENABLE START] attempt=${attempt + 1}');
      await participant.setMicrophoneEnabled(true);
      final hasLiveAudio = participant.trackPublications.values.any(
        (publication) =>
            publication.kind == TrackType.AUDIO && !publication.muted,
      );
      if (hasLiveAudio) {
        print('[CN CALL][MIC ENABLE RESULT] enabled=true');
        print('[CN CALL][LOCAL AUDIO PUBLISHED]');
        return;
      }
      print('[CN CALL][MIC ENABLE RESULT] enabled=false');
      await Future<void>.delayed(const Duration(milliseconds: 300));
    }
    print('[CN CALL][LIVEKIT CONNECT FAILED] microphone_not_published');
    throw StateError('LiveKit did not publish an unmuted local audio track; check RECORD_AUDIO permission');
  }
}
