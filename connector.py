from connectors.core.connector import Connector, ConnectorError, get_logger

from .operations import (
    publish_message,
    publish_json,
    publish_base64,
    subscribe_once,
    subscribe_once_json,
    subscribe_multi_once,
    subscribe_stream,
    unsubscribe,
    clear_retained,
    connection_status,
    topic_match,
    get_last_will,
    set_last_will_config,
    check_health,
)

logger = get_logger("mqtt_paho")


class MqttPahoConnector(Connector):
    def execute(self, config, operation, params, **kwargs):
        supported_operations = {
            "publish_message": publish_message,
            "publish_json": publish_json,
            "publish_base64": publish_base64,
            "subscribe_once": subscribe_once,
            "subscribe_once_json": subscribe_once_json,
            "subscribe_multi_once": subscribe_multi_once,
            "subscribe_stream": subscribe_stream,
            "unsubscribe": unsubscribe,
            "clear_retained": clear_retained,
            "connection_status": connection_status,
            "topic_match": topic_match,
            "get_last_will": get_last_will,
            "set_last_will_config": set_last_will_config,
        }

        operation_fn = supported_operations.get(operation)
        if not operation_fn:
            raise ConnectorError("Unsupported operation: {0}".format(operation))

        return operation_fn(config, params)

    def check_health(self, config):
        return check_health(config)
