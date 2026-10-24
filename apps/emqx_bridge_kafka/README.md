# EMQX Kafka Bridge

This application provides a Kafka **producer** connector and action for EMQX, so that
messages can be forwarded to Kafka from the rule engine.

## Configuration

The connector and the action are configured under the standard v2 roots:

```hocon
connectors {
  kafka_producer {
    my_kafka {
      bootstrap_hosts = "kafka-1.emqx.net:9092,kafka-2.emqx.net:9092"
      connect_timeout = 5s
      min_metadata_refresh_interval = 3s
      request_timeout = 30s
      authentication = none
      socket_opts {
        nodelay = true
        sndbuf = 1024KB
        recbuf = 1024KB
        tcp_keepalive = none
      }
      ssl { enable = false }
    }
  }
}

actions {
  kafka_producer {
    my_kafka_action {
      connector = "my_kafka"
      parameters {
        topic = "my-topic"
        message {
          key = "${.clientid}"
          value = "${.}"
          timestamp = "${.timestamp}"
        }
      }
    }
  }
}
```

The action can be used from a rule. Rules may reference the action by its v2 type
(`kafka_producer:<action>`) or by the legacy alias `kafka:<action>`:

```hocon
rule_engine {
  rules.my_kafka_rule {
    sql = "SELECT payload FROM \"t/#\""
    actions = ["kafka:my_kafka_action"]
  }
}
```

`parameters` supports the following keys:

| Key | Description |
| --- | --- |
| `topic` | Kafka topic, may be a template (e.g. `"${clientid}/topic"`). |
| `message.key` / `message.value` / `message.timestamp` | Templates for the produced record. |
| `kafka_headers` | Template which renders to a map of Kafka headers (defaults to `${pub_props}`). |
| `kafka_ext_headers` | List of extra `kafka_ext_header_key`/`kafka_ext_header_value` pairs. |
| `kafka_header_value_encode_mode` | `none` (drop non-binary values) or `json` (JSON encode them). |
| `compression` | `no_compression`, `snappy` or `gzip`. |
| `required_acks` | `all_isr`, `leader_only` or `none`. |
| `partition_strategy` | `random` or `key_dispatch`. |
| `partitions_limit` | `all_partitions` (default) or a positive integer. |
| `buffer` | `mode` is `memory` (default), `disk` or `hybrid`; also `per_partition_limit`, `segment_bytes` and `memory_overload_protection`. |
| `max_batch_bytes`, `max_inflight`, `max_linger_time`, `max_linger_bytes` | Producer batching options. |

## Design notes

* **Producer only.** A Kafka consumer (source) is not implemented; use an MQTT source
  (or any other source) and a `kafka_producer` action to forward messages to Kafka.
* **Buffering** is delegated to the underlying `wolff` producer, which uses `replayq`.
  The resource query modes `simple_sync_internal_buffer`/`simple_async_internal_buffer`
  are used so that the EMQX resource buffer does not buffer on top of `replayq`.
  When Kafka becomes unreachable, queries keep flowing into `replayq` and the connector
  and action statuses move to `connecting` instead of `disconnected`.
* **Sync or async** is chosen with the standard `resource_opts.query_mode`
  (`async` by default) and `resource_opts.request_ttl`. In `sync` mode the action waits
  for the Kafka acknowledgement (up to `request_ttl`) before the rule engine considers
  the message sent.
* On-disk buffering (`parameters.buffer.mode = disk`/`hybrid`) is not allowed together
  with a dynamic (templated) topic, because the set of topics is not known when the
  per-partition `replayq` files are created.
* `request_timeout` is used both for Kafka requests and for metadata requests, as the
  client library does not take a separate metadata request timeout.

## Tests

* Unit tests (no broker required):

  ```
  ./rebar3 eunit --module=emqx_bridge_kafka_schema_tests
  ./rebar3 eunit --module=emqx_bridge_kafka_impl_tests
  ./rebar3 eunit --module=emqx_bridge_kafka_info_tests
  ```

* Common tests need a Kafka cluster (and `toxiproxy` for the failure tests):

  ```
  ./scripts/ct/run.sh --app apps/emqx_bridge_kafka
  ```

  To run the v2 producer suite directly against services on the host:

  ```sh
  PROFILE=emqx \
  KAFKA_BOOTSTRAP_HOSTS=127.0.0.1:29092 \
  KAFKA_SASL_BOOTSTRAP_HOSTS=127.0.0.1:29095 \
  KAFKA_PROXY_BOOTSTRAP_HOSTS=127.0.0.1:29292 \
  PROXY_HOST=127.0.0.1 PROXY_PORT=28474 \
  SUITE=apps/emqx_bridge_kafka/test/emqx_bridge_kafka_v2_producer_SUITE.erl \
  make ct-suite
  ```

  The plain and SCRAM-SHA-512 listeners must advertise addresses reachable from
  the test process. The SCRAM listener needs user `emqxuser` with password
  `password`. Pre-create `test-topic-one-partition` with one partition. Toxiproxy
  needs proxies named `kafka_plain` and `kafka_2_plain`; the proxied Kafka listener
  must also advertise a proxy address so metadata cannot bypass fault injection.
  `TESTCASE=t_send_message,t_dynamic_topic` selects individual cases.

  This suite checks real MQTT QoS 0/1/2 delivery, Kafka record keys, values and
  headers, sync/async sends, dynamic topics, SCRAM authentication, connector
  restarts, outage buffering and disk/hybrid replay after a connector restart.
