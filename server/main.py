        """,
        (event_id,),
    ).fetchone()
    db.close()

    if row is None:
        return True

    if int(row["attempt_count"] or 0) > 0:
        return False

    target_id = str(row["target_user_id"])

    # Consume the one allowed delivery attempt before touching the transport.
    _mark_terminal_attempt(event_id)

    target_socket = connections.get(target_id)

    if target_socket is not None:
        payload = {
            "type": str(row["event_type"]),
            "call_id": str(row["call_id"]),
            "target_id": target_id,
            "from_id": str(row["source_user_id"]),
            "event_id": str(row["event_id"]),
        }

        # Offline-after-grace is represented by a durable terminal
        # call_reject event plus a missed call record. Preserve the reason in
        # the delivered frame so Android can select offline.mp3.
        if str(row["event_type"]) == "call_reject":
            status_db = get_db()
            try:
                status_row = status_db.execute(
                    "SELECT status FROM call_records WHERE call_id = ?",
                    (str(row["call_id"]),),
                ).fetchone()
            finally:
                status_db.close()

            if status_row is not None and str(status_row["status"]) == "missed":
                payload["reason"] = "offline"

        try:
            await target_socket.send_json(payload)
            print(
                "[CN CALL][DURABLE TERMINAL WS SENT]",
                row["event_type"],
                "call_id=", row["call_id"],
                "event_id=", event_id,
            )
            return True
        except Exception as exc:
            print(
                "[CN CALL][DURABLE TERMINAL WS ERROR]",
                "call_id=", row["call_id"],
                "event_id=", event_id,
                "error=", exc,