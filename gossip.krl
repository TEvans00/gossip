ruleset gossip {
  meta {
    name "Gossip"
    description <<
      Allows temperature sensors to gossip about their temperature readings
    >>
    author "Tyla Evans"
    use module temperature_store alias temp_store
    use module io.picolabs.subscription alias subs
    shares name, temp_log, seen, seen_by_me
  }

  global {
    nodes = function() {
      subs:established().filter(
        function(sub){
          (sub{"Tx_role"} == "node") && (sub{"Rx_role"} == "node")
        }
      ).map(
        function(node){
          eci = node{"Tx"}
          host = node{"Tx_host"}.defaultsTo(meta:host)
          seen = ent:seen{eci}.defaultsTo({})
          return { "eci": eci, "Rx": node{"Rx"}, "host": host, "seen": seen }
        }
      )
    }

    generateMessagesNeeded = function(node_id, max, num) {
      num <= max =>
        [{"node_id": node_id, "num": num}].append(generateMessagesNeeded(node_id, max, num + 1)) |
        []
    }

    chooseRandomFromArray = function(array) {
      array[random:integer(array.length() - 1)]
    }

    calculateMessagesNeeded = function(seen) {
      ent:my_seen.keys().reduce(
        function(acc, node_id) {
          my_seen = ent:my_seen{node_id}
          node_seen = seen{node_id}.defaultsTo(-1)
          acc.append(generateMessagesNeeded(node_id, my_seen, node_seen + 1))
        }, [])
    }

    choosePeerForRumor = function() {
      sortedNodes = nodes().map(
        function(node){
          node.put(["messages_needed"], calculateMessagesNeeded(node{"seen"})).klog("node with messages needed:")
        }
      ).sort(function(a, b) {
        a{"messages_needed"}.length() > b{"messages_needed"}.length()  => -1 |
          a{"messages_needed"}.length() == b{"messages_needed"}.length() =>  0 |
            1
      })
      maxWeight = sortedNodes.head(){"messages_needed"}.length().klog("max weight:")
      maxWeight > 0 =>
        chooseRandomFromArray(sortedNodes.filter(function(node) {node{"messages_needed"}.length() == maxWeight})) |
        null
    }

    choosePeerForSeen = function() {
      sortedNodes = nodes().map(
        function(node){
          weight = (node{"seen"}.keys().reduce(
            function(acc, node_id) {
              my_seen = ent:my_seen{node_id}.defaultsTo(-1)
              node_seen = node{"seen"}{node_id}
              node_seen > my_seen => acc + (node_seen - my_seen) | acc
            }, 0))
          node.put(["weight"], weight).klog("weighted node:")
        }
      ).sort(function(a, b) {
        a{"weight"} > b{"weight"}  => -1 |
          a{"weight"} == b{"weight"} =>  0 |
            1
      })
      maxWeight = sortedNodes.head(){"weight"}.klog("max weight:")
      chooseRandomFromArray(sortedNodes.filter(function(node) {node{"weight"} == maxWeight}))
    }

    prepareMessage = function(node_id, num) {
      message_id = node_id + ":" + num
      ent:temp_log{node_id}{message_id}
    }

    calculateLastSeen = function(node_id, num) {
      next_message_id = (node_id + ":" + (num + 1)).klog("checking for message:")
      ent:temp_log{node_id} >< next_message_id =>
        calculateLastSeen(node_id, num + 1) |
        num
    }

    name = function() {
      ent:name
    }

    temp_log = function() {
      ent:temp_log
    }

    seen = function() {
      ent:seen
    }

    seen_by_me = function() {
      ent:my_seen
    }
  }

  rule initialization {
    select when wrangler ruleset_installed where event:attrs{"rids"} >< meta:rid
    pre {
      name = random:word().klog("name:")
    }
    always {
      ent:name := name
      ent:sequence_num := 0
      ent:seen := {}
      ent:temp_log := {}
      ent:my_seen := {}
      ent:process := true
    }
  }

  rule reset {
    select when gossip reset
    always {
      ent:sequence_num := 0
      ent:seen := {}
      ent:temp_log := {}
      ent:my_seen := {}
      ent:process := true
    }
  }

  rule record_new_temp {
    select when wovyn new_temperature_reading
      temperature re#^(\d+[.]?\d*)$#
      timestamp re#^(.+)$#
      setting(temp, time)
    pre {
      message_id = ent:name + ":" + ent:sequence_num
      message = {
        "MessageID": message_id,
        "SensorID": ent:name,
        "Temperature": temp,
        "Timestamp":  time
      }.klog("message:")
    }
    if ent:process then noop()
    fired {
      ent:temp_log := ent:temp_log.put([ent:name, message_id], message)
      ent:my_seen{ent:name} := ent:sequence_num
    }
  }

  rule start_heartbeat {
    select when gossip start_heartbeat
    pre {
      period = (event:attrs{"period"} && event:attrs{"period"} != "" => event:attrs{"period"} | 30).klog("period:")
    }
    if ent:heartbeat_schedule then send_directive("heartbeat_already_running", {"heartbeat": ent:heartbeat_schedule})
    notfired {
      schedule gossip event "heartbeat"
        repeat << */#{period} * * * * * >>  attributes { } setting(heartbeat);
      ent:heartbeat_schedule := heartbeat
    }
  }

  rule stop_heartbeat {
    select when gossip stop_heartbeat
    if ent:heartbeat_schedule then schedule:remove(ent:heartbeat_schedule)
    fired {
      ent:heartbeat_schedule := null
    }
  }

  rule adjust_heartbeat {
    select when gossip adjust_heartbeat
      period re#^(\d+)$#
      setting (period)
    pre {
      new_period = period.klog("period:")
    }
    if ent:heartbeat_schedule then schedule:remove(ent:heartbeat_schedule)
    always {
      schedule gossip event "heartbeat"
        repeat << */#{period} * * * * * >>  attributes { } setting(id);
      ent:heartbeat_schedule := id
    }
  }

  rule trigger_message {
    select when gossip heartbeat
    pre {
      message_type = random:integer(2)
    }
    if message_type then noop()
    fired {
      raise seen_message event "selected"
    } else {
      raise rumor_message event "selected"
    }
  }

  rule prepare_rumor {
    select when rumor_message:selected
    pre {
      subscriber = choosePeerForRumor().klog("subscriber:")
      message_chosen = subscriber => chooseRandomFromArray(subscriber{"messages_needed"}).klog("message chosen:") | null
    }
    if subscriber && message_chosen && ent:process then noop()
    fired {
      raise rumor event "send_request"
        attributes {
          "receiver": subscriber,
          "messages": [message_chosen]
        }
    }
  }

  rule send_rumor {
    select when rumor:send_request
    foreach event:attrs{"messages"} setting(message_info)
    pre {
      receiver = event:attrs{"receiver"}.klog("receiver:")
      eci = receiver{"eci"}
      node_id = message_info{"node_id"}.klog("node id:")
      num = message_info{"num"}.klog("num:")
      message = prepareMessage(node_id, num).klog("message:")
      last_seen = ent:seen{eci}{node_id}.defaultsTo(-1)
    }
    event:send({
      "eci":eci,
      "domain":"gossip",
      "type":"rumor",
      "attrs": {"message": message}
    }, receiver{"host"})
    fired {
      ent:sequence_num := ((node_id == ent:name) && (num == ent:sequence_num)).klog("update sequence num:") =>
        ent:sequence_num + 1 |
        ent:sequence_num
      ent:seen := (num > last_seen) =>
        ent:seen.put([eci, node_id], num) |
        ent:seen
    }
  }

  rule send_seen {
    select when seen_message:selected
    pre {
      receiver = choosePeerForSeen().klog("receiver:")
      message = ent:my_seen.klog("message:")
    }
    if receiver && ent:process then
      event:send({
        "eci":receiver{"eci"},
        "domain":"gossip",
        "type":"seen",
        "attrs": {"message": message}
      }, receiver{"host"})
  }

  rule update_log {
    select when gossip:rumor
    pre {
      message = event:attrs{"message"}
      num = (message{"MessageID"}.split(re#:#)[1].as("Number")).klog("num:")
      sensor_id = message{"SensorID"}.klog("sensor id:")
      last_seen = (ent:my_seen{sensor_id}.defaultsTo(-1)).klog("last seen:")
    }
    if ent:process then noop()
    fired {
      ent:temp_log := ent:temp_log.put([sensor_id, message{"MessageID"}], message)
      ent:my_seen := (num == (last_seen + 1)) =>
        ent:my_seen.put([sensor_id], calculateLastSeen(sensor_id, num)) |
        ent:my_seen
    }
  }

  rule update_peer {
    select when gossip:seen
    pre {
      messages_needed = calculateMessagesNeeded(event:attrs{"message"}).klog("messages needed:")
      node = (nodes().filter(
        function(node){
          node{"Rx"} == event:eci
        }
      )[0]).klog("node:")
    }
    if ent:process then noop()
    fired {
      ent:seen := ent:seen.put([node{"eci"}], event:attrs{"message"})
      raise rumor event "send_request"
        attributes {
          "receiver": node,
          "messages": messages_needed
        }
    }
  }

  rule toggle_process {
    select when gossip:process
      status re#^(off|on)$#
      setting(status)
    pre {
      value = (status == "on" => true | false).klog("value:")
    }
    always {
      ent:process := value
    }
  }
}
