// ignore_for_file: avoid_print

import 'dart:async';

enum CoordinatedCallState {
  idle,
  outgoing,
  ringing,
  accepting,
  negotiating,
  connecting,
  active,
  ending,
  ended,
}

enum CallCommandResult {
  accepted,
  alreadyProcessing,
  blockedByActiveCall,
  staleCall,
}

class CallCoordinator {
  CallCoordinator._();

  static final CallCoordinator instance = CallCoordinator._();

  CoordinatedCallState _state = CoordinatedCallState.idle;
  String? _callId;

  // Every state-changing operation is chained through this queue.
  // This prevents accept/reject/hangup/start from executing concurrently.
  Future<void> _queue = Future<void>.value();

  CoordinatedCallState get state => _state;
  String? get callId => _callId;

  bool get hasActiveCall =>
      _callId != null &&
      _state != CoordinatedCallState.idle &&
      _state != CoordinatedCallState.ended;

  bool owns(String id) => _callId == id;

  Future<T> serialize<T>(
    Future<T> Function() operation,
  ) {
    final previous = _queue;

    final completer = Completer<T>();

    _queue = () async {
      try {
        await previous;
        final result = await operation();
        if (!completer.isCompleted) {
          completer.complete(result);
        }
      } catch (error, stackTrace) {
        if (!completer.isCompleted) {
          completer.completeError(error, stackTrace);
        }
      }
    }();

    return completer.future;
  }

  CallCommandResult beginOutgoing(String id) {
    final safeId = id.trim();
    if (safeId.isEmpty) {
      return CallCommandResult.staleCall;
    }

    if (hasActiveCall) {
      return CallCommandResult.blockedByActiveCall;
    }

    _callId = safeId;
    _state = CoordinatedCallState.outgoing;

    print(
      '[CN CALL][COORDINATOR] outgoing '
      'call_id=$safeId',
    );

    return CallCommandResult.accepted;
  }


  CallCommandResult beginConnecting(String id) {
    if (!owns(id)) {
      return CallCommandResult.staleCall;
    }

    if (_state == CoordinatedCallState.ending ||
        _state == CoordinatedCallState.ended ||
        _state == CoordinatedCallState.active) {
      return CallCommandResult.staleCall;
    }

    _state = CoordinatedCallState.connecting;

    return CallCommandResult.accepted;
  }

  CallCommandResult markActive(String id) {
    if (!owns(id)) {
      return CallCommandResult.staleCall;
    }

    if (_state == CoordinatedCallState.ending ||
        _state == CoordinatedCallState.ended) {
      return CallCommandResult.staleCall;
    }

    _state = CoordinatedCallState.active;

    print(
      '[CN CALL][COORDINATOR] active '
      'call_id=$id',
    );

    return CallCommandResult.accepted;
  }

  CallCommandResult beginEnding(String id) {
    if (!owns(id)) {
      return CallCommandResult.staleCall;
    }

    if (_state == CoordinatedCallState.ending ||
        _state == CoordinatedCallState.ended) {
      return CallCommandResult.alreadyProcessing;
    }

    _state = CoordinatedCallState.ending;

    print(
      '[CN CALL][COORDINATOR] ending '
      'call_id=$id',
    );

    return CallCommandResult.accepted;
  }

  void markEnded(String id) {
    if (!owns(id)) {
      return;
    }

    _state = CoordinatedCallState.ended;

    print(
      '[CN CALL][COORDINATOR] ended '
      'call_id=$id',
    );

    // The identity is deliberately cleared only after the terminal state
    // has been recorded. This prevents a late event from attaching itself
    // to the next call.
    _callId = null;

    _state = CoordinatedCallState.idle;
  }

  void reset() {
    _callId = null;
    _state = CoordinatedCallState.idle;
  }
}
