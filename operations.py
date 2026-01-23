import base64
import json
import time
import queue
import threading

import paho.mqtt.client as mqtt

from connectors.core.connector import ConnectorError, get_logger

logger = get_logger("mqtt_paho")


def _to_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "y", "on")
    return bool(value)


def _to_int(value, default=0):
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _get_value(config, key, default=None):
    value = config.get(key)
    if value is None or value == "":
        return default
    return value


def _build_client(config):
    client_id = _get_value(config, "client_id", "")
    clean_session = _to_bool(_get_value(config, "clean_session", True), True)

    client = mqtt.Client(
        client_id=client_id,
        clean_session=clean_session,
        protocol=mqtt.MQTTv311,
    )

    username = _get_value(config, "username")
    password = _get_value(config, "password")
    if username:
        client.username_pw_set(username=username, password=password or None)

    will_topic = _get_value(config, "will_topic")
    if will_topic:
        will_payload = _get_value(config, "will_payload", "")
        will_qos = _to_int(_get_value(config, "will_qos", 0), 0)
        will_retain = _to_bool(_get_value(config, "will_retain", False), False)
        client.will_set(will_topic, payload=will_payload, qos=will_qos, retain=will_retain)

    use_tls = _to_bool(_get_value(config, "use_tls", False), False)
    if use_tls:
        ca_cert = _get_value(config, "ca_cert")
        client.tls_set(ca_certs=ca_cert if ca_cert else None)
        verify_ssl = _to_bool(_get_value(config, "verify_ssl", True), True)
        if not verify_ssl:
            client.tls_insecure_set(True)

    return client


def _connect_client(client, config, timeout):
    host = _get_value(config, "broker_host")
    port = _to_int(_get_value(config, "broker_port", 1883), 1883)
    keepalive = _to_int(_get_value(config, "keepalive", 60), 60)

    connected = threading.Event()
    result = {"rc": None}

    def on_connect(_client, _userdata, _flags, rc):
        result["rc"] = rc
        connected.set()

    client.on_connect = on_connect
    client.connect(host, port, keepalive)
    client.loop_start()

    if not connected.wait(timeout):
        raise ConnectorError("Timed out connecting to MQTT broker.")
    if result["rc"] != 0:
        raise ConnectorError("MQTT connection failed with code: {0}".format(result["rc"]))
    return result["rc"]


def _disconnect_client(client):
    try:
        client.disconnect()
    finally:
        client.loop_stop()


def check_health(config):
    client = _build_client(config)
    timeout = _to_int(_get_value(config, "connect_timeout", 10), 10)
    try:
        _connect_client(client, config, timeout)
        return True
    except Exception as exc:
        logger.exception("MQTT health check failed: %s", exc)
        raise ConnectorError("MQTT health check failed: {0}".format(exc))
    finally:
        _disconnect_client(client)


def _publish_payload(config, params, payload):
    client = _build_client(config)
    timeout = _to_int(_get_value(params, "timeout", 10), 10)
    topic = _get_value(params, "topic")
    qos = _to_int(_get_value(params, "qos", 0), 0)
    retain = _to_bool(_get_value(params, "retain", False), False)

    if not topic:
        raise ConnectorError("Topic is required to publish a message.")

    try:
        _connect_client(client, config, _to_int(_get_value(config, "connect_timeout", 10), 10))
        info = client.publish(topic, payload=payload, qos=qos, retain=retain)
        info.wait_for_publish(timeout=timeout)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise ConnectorError("Publish failed with code: {0}".format(info.rc))

        return {
            "status": "success",
            "message": "Message published.",
            "topic": topic,
            "mid": str(info.mid),
            "published": "true",
        }
    except Exception as exc:
        logger.exception("Publish failed: %s", exc)
        raise ConnectorError("Publish failed: {0}".format(exc))
    finally:
        _disconnect_client(client)


def publish_message(config, params):
    payload = _get_value(params, "payload", "")
    return _publish_payload(config, params, payload)


def publish_json(config, params):
    json_payload = _get_value(params, "json_payload")
    if not json_payload:
        raise ConnectorError("JSON payload is required.")
    try:
        parsed = json.loads(json_payload)
    except Exception as exc:
        raise ConnectorError("Invalid JSON payload: {0}".format(exc))

    serialized = json.dumps(parsed, separators=(",", ":"), ensure_ascii=True)
    return _publish_payload(config, params, serialized)


def publish_base64(config, params):
    payload_b64 = _get_value(params, "payload_b64")
    if not payload_b64:
        raise ConnectorError("Base64 payload is required.")
    try:
        payload = base64.b64decode(payload_b64.encode("ascii"), validate=True)
    except Exception as exc:
        raise ConnectorError("Invalid base64 payload: {0}".format(exc))

    return _publish_payload(config, params, payload)


def _parse_topics(topics_value):
    if isinstance(topics_value, list):
        return [topic for topic in topics_value if topic]
    if not topics_value:
        return []
    text = topics_value.strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [topic for topic in parsed if topic]
        except Exception:
            pass
    parts = [topic.strip() for topic in text.replace(",", "\n").splitlines()]
    return [topic for topic in parts if topic]


def _format_payload(payload, payload_format):
    if payload_format == "base64":
        return {"payload": "", "payload_b64": base64.b64encode(payload).decode("ascii")}
    if payload_format == "json":
        payload_text = payload.decode("utf-8", errors="replace")
        try:
            parsed = json.loads(payload_text)
        except Exception as exc:
            raise ConnectorError("Received payload is not valid JSON: {0}".format(exc))
        return {"payload_json": json.dumps(parsed, ensure_ascii=True)}
    payload_text = payload.decode("utf-8", errors="replace")
    return {"payload": payload_text, "payload_b64": ""}


def _subscribe_once_message(config, params):
    client = _build_client(config)
    timeout = _to_int(_get_value(params, "timeout", 30), 30)
    topic = _get_value(params, "topic")
    qos = _to_int(_get_value(params, "qos", 0), 0)

    if not topic:
        raise ConnectorError("Topic is required to subscribe.")

    message_queue = queue.Queue(maxsize=1)

    def on_message(_client, _userdata, message):
        if message_queue.full():
            return
        message_queue.put(message)

    try:
        _connect_client(client, config, _to_int(_get_value(config, "connect_timeout", 10), 10))
        client.on_message = on_message
        client.subscribe(topic, qos=qos)

        return message_queue.get(timeout=timeout)
    except queue.Empty:
        raise ConnectorError("Timed out waiting for a message.")
    except Exception as exc:
        logger.exception("Subscribe failed: %s", exc)
        raise ConnectorError("Subscribe failed: {0}".format(exc))
    finally:
        _disconnect_client(client)


def subscribe_once(config, params):
    message = _subscribe_once_message(config, params)
    decode_payload = _to_bool(_get_value(params, "decode_payload", True), True)
    payload_text = ""
    payload_b64 = ""
    if decode_payload:
        payload_text = message.payload.decode("utf-8", errors="replace")
    else:
        payload_b64 = base64.b64encode(message.payload).decode("ascii")

    return {
        "status": "success",
        "message": "Message received.",
        "topic": message.topic,
        "payload": payload_text,
        "payload_b64": payload_b64,
        "qos": str(message.qos),
        "retain": str(message.retain),
    }


def subscribe_once_json(config, params):
    message = _subscribe_once_message(config, params)
    payload_text = message.payload.decode("utf-8", errors="replace")
    try:
        parsed = json.loads(payload_text)
    except Exception as exc:
        raise ConnectorError("Received payload is not valid JSON: {0}".format(exc))

    return {
        "status": "success",
        "message": "JSON message received.",
        "topic": message.topic,
        "payload_json": json.dumps(parsed, ensure_ascii=True),
        "qos": str(message.qos),
        "retain": str(message.retain),
    }


def subscribe_multi_once(config, params):
    client = _build_client(config)
    timeout = _to_int(_get_value(params, "timeout", 30), 30)
    topics_value = _get_value(params, "topics")
    qos = _to_int(_get_value(params, "qos", 0), 0)
    payload_format = _get_value(params, "payload_format", "text")

    topics = _parse_topics(topics_value)
    if not topics:
        raise ConnectorError("Topics are required to subscribe.")

    message_queue = queue.Queue(maxsize=1)

    def on_message(_client, _userdata, message):
        if message_queue.full():
            return
        message_queue.put(message)

    try:
        _connect_client(client, config, _to_int(_get_value(config, "connect_timeout", 10), 10))
        client.on_message = on_message
        client.subscribe([(topic, qos) for topic in topics])
        message = message_queue.get(timeout=timeout)
        payload_fields = _format_payload(message.payload, payload_format)
        return {
            "status": "success",
            "message": "Message received.",
            "topic": message.topic,
            "qos": str(message.qos),
            "retain": str(message.retain),
            **payload_fields,
        }
    except queue.Empty:
        raise ConnectorError("Timed out waiting for a message.")
    except Exception as exc:
        logger.exception("Subscribe failed: %s", exc)
        raise ConnectorError("Subscribe failed: {0}".format(exc))
    finally:
        _disconnect_client(client)


def subscribe_stream(config, params):
    client = _build_client(config)
    timeout = _to_int(_get_value(params, "timeout", 30), 30)
    max_messages = _to_int(_get_value(params, "max_messages", 10), 10)
    topic = _get_value(params, "topic")
    qos = _to_int(_get_value(params, "qos", 0), 0)
    payload_format = _get_value(params, "payload_format", "text")

    if not topic:
        raise ConnectorError("Topic is required to subscribe.")
    if max_messages < 1:
        raise ConnectorError("max_messages must be at least 1.")

    messages = []
    message_queue = queue.Queue()
    deadline = time.time() + timeout

    def on_message(_client, _userdata, message):
        message_queue.put(message)

    try:
        _connect_client(client, config, _to_int(_get_value(config, "connect_timeout", 10), 10))
        client.on_message = on_message
        client.subscribe(topic, qos=qos)

        while len(messages) < max_messages and time.time() < deadline:
            remaining = max(0.0, deadline - time.time())
            try:
                message = message_queue.get(timeout=remaining)
            except queue.Empty:
                break
            payload_fields = _format_payload(message.payload, payload_format)
            messages.append(
                {
                    "topic": message.topic,
                    "qos": str(message.qos),
                    "retain": str(message.retain),
                    **payload_fields,
                }
            )

        return {
            "status": "success",
            "message": "Messages received.",
            "count": str(len(messages)),
            "messages": messages,
        }
    except Exception as exc:
        logger.exception("Subscribe stream failed: %s", exc)
        raise ConnectorError("Subscribe stream failed: {0}".format(exc))
    finally:
        _disconnect_client(client)


def unsubscribe(config, params):
    client = _build_client(config)
    topics_value = _get_value(params, "topics")
    topics = _parse_topics(topics_value)
    if not topics:
        raise ConnectorError("Topics are required to unsubscribe.")

    try:
        _connect_client(client, config, _to_int(_get_value(config, "connect_timeout", 10), 10))
        result, mid = client.unsubscribe(topics)
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise ConnectorError("Unsubscribe failed with code: {0}".format(result))
        return {
            "status": "success",
            "message": "Unsubscribed.",
            "mid": str(mid),
        }
    except Exception as exc:
        logger.exception("Unsubscribe failed: %s", exc)
        raise ConnectorError("Unsubscribe failed: {0}".format(exc))
    finally:
        _disconnect_client(client)


def clear_retained(config, params):
    topic = _get_value(params, "topic")
    if not topic:
        raise ConnectorError("Topic is required to clear retained message.")
    params_with_retain = dict(params)
    params_with_retain["retain"] = True
    params_with_retain["payload"] = ""
    return _publish_payload(config, params_with_retain, "")


def connection_status(config, params):
    client = _build_client(config)
    timeout = _to_int(_get_value(config, "connect_timeout", 10), 10)
    try:
        rc = _connect_client(client, config, timeout)
        return {
            "status": "success",
            "message": "Connected.",
            "rc": str(rc),
        }
    except Exception as exc:
        logger.exception("Connection status failed: %s", exc)
        raise ConnectorError("Connection status failed: {0}".format(exc))
    finally:
        _disconnect_client(client)


def topic_match(config, params):
    subscription = _get_value(params, "subscription")
    topic = _get_value(params, "topic")
    if not subscription or not topic:
        raise ConnectorError("subscription and topic are required.")
    is_match = mqtt.topic_matches_sub(subscription, topic)
    return {
        "status": "success",
        "message": "Match evaluated.",
        "matches": str(bool(is_match)).lower(),
    }


def get_last_will(config, params):
    return {
        "status": "success",
        "message": "Last will configuration.",
        "will_topic": _get_value(config, "will_topic", ""),
        "will_payload": _get_value(config, "will_payload", ""),
        "will_qos": str(_to_int(_get_value(config, "will_qos", 0), 0)),
        "will_retain": str(_to_bool(_get_value(config, "will_retain", False), False)).lower(),
    }


def set_last_will_config(config, params):
    update_config = {
        "will_topic": _get_value(params, "will_topic", ""),
        "will_payload": _get_value(params, "will_payload", ""),
        "will_qos": _to_int(_get_value(params, "will_qos", 0), 0),
        "will_retain": _to_bool(_get_value(params, "will_retain", False), False),
    }
    config_id = _get_value(params, "config_id")
    try:
        from integrations.connectors.core.utils import update_connnector_config
    except Exception:
        raise ConnectorError("update_connnector_config is not available on this system.")

    update_connnector_config("mqtt_paho", "1.0.0", update_config, config_id)
    return {
        "status": "success",
        "message": "Last will configuration updated.",
    }
