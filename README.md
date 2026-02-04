MQTT Paho Connector (FortiSOAR 7.6.5)
======================================

Summary
-------
This connector integrates FortiSOAR with MQTT brokers using the Eclipse Paho
client. It supports publishing and subscribing to topics, collecting streaming
messages, managing retained messages, and working with Last Will settings.
Configuration includes TLS, authentication, client ID, keepalive, and connect
timeouts.

Included Actions
----------------
- Publish Message
- Publish JSON
- Publish Base64
- Subscribe Once
- Subscribe Once (JSON)
- Subscribe Multiple (Once)
- Subscribe Stream
- Unsubscribe
- Clear Retained Message
- Connection Status
- Topic Match
- Get Last Will
- Set Last Will

Package Contents
----------------
- Connector source: `mqtt_paho/`
- Icons: `mqtt_paho/images/`
- Sample playbooks: `mqtt_paho/playbooks/playbooks.json`
- Packaged archive: `mqtt_paho-1.0.1.tgz`
