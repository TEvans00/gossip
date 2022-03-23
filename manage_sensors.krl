ruleset manage_sensors {
  meta {
    name "Manage Sensors"
    description <<
      Manages a collection of temperature sensors
    >>
    author "Tyla Evans"
    provides sensors, temperatures, reports
    shares sensors, temperatures, reports
    use module io.picolabs.wrangler alias wrangler
    use module io.picolabs.subscription alias subs
  }

  global {
    default_threshold = 78

    sensors = function() {
      subs:established().filter(
        function(sub){
          sub{"Tx_role"} == "sensor"
        }
      ).map(
        function(sensor){
          eci = sensor{"Tx"}
          host = sensor{"Tx_host"}.defaultsTo(meta:host)
          profile = wrangler:picoQuery(eci,"sensor_profile","profile",{},host)
          info = sensor.put("name", profile{"name"})
          info
        }
      )
    }

    temperatures = function() {
      return sensors().reduce(
        function(acc, sensor){
          eci = sensor{"Tx"}.klog("eci:")
          host = (sensor{"Tx_host"}.defaultsTo(meta:host)).klog("host:")
          name = sensor{"name"}.klog("name:")
          temperatures = wrangler:picoQuery(eci,"temperature_store","temperatures",{}, host).klog("temperatures:")
          return acc.put(eci, {"name": name, "temperatures": temperatures})
        }, {})
    }

    reports = function() {
      num = ((ent:reports.length() < 5) => (ent:reports.length() - 1) | 4).klog("num reports:")
      report_ids = (ent:reports.keys().reverse().slice(num))
      ent:reports.filter(function(v,k){report_ids >< k})
    }
  }

  rule intialization {
    select when wrangler ruleset_installed where event:attrs{"rids"} >< meta:rid
    always {
      ent:eci_to_name := {}
      ent:reports := {}
      ent:report_id := 0
    }
  }

  rule create_sensor {
    select when sensor new_sensor
    pre {
      name = event:attrs{"name"}.klog("name:")
    }
    send_directive("initializing_sensor", {"sensor_name":name})
    always {
      raise wrangler event "new_child_request"
        attributes { "name": name,
                     "backgroundColor": "#13A169" }
    }
  }

  rule store_sensor_name {
    select when wrangler new_child_created
    pre {
      eci = event:attrs{"eci"}.klog("eci:")
      name = event:attrs{"name"}.klog("name:")
    }
    if eci && name then noop()
    fired {
      ent:eci_to_name{eci} := name
    }
  }

  rule trigger_sensor_installation {
    select when wrangler new_child_created
    pre {
      eci = event:attrs{"eci"}.klog("eci:")
    }
    if eci then
      event:send(
        { "eci": eci,
          "eid": "install-ruleset",
          "domain": "wrangler",
          "type": "install_ruleset_request",
          "attrs": {
            "absoluteURL": meta:rulesetURI,
            "rid": "sensor_installer"
          }
        }
      )
  }

  rule introduce_sensor {
    select when sensor introduction
      wellKnown_eci re#(.+)#
      setting(wellKnown_eci)
      pre {
        host = event:attrs{"host"}
      }
      if host && host != "" then noop()
      fired {
        raise sensor event "subscription_request" attributes {
          "wellKnown_eci": wellKnown_eci,
          "host": host
        }
      } else {
        raise sensor event "subscription_request" attributes {
          "wellKnown_eci": wellKnown_eci
        }
      }
  }

  rule initiate_subscription_to_child_sensor {
    select when sensor_installer installation_finished
      wellKnown_eci re#(.+)#
      setting(wellKnown_eci)
    always {
      raise sensor event "subscription_request" attributes {
        "wellKnown_eci": wellKnown_eci
      }
    }
  }

  rule subscribe_to_sensor {
    select when sensor subscription_request
      wellKnown_eci re#(.+)#
      setting(wellKnown_eci)
    pre {
      host = event:attrs{"host"}
    }
    always {
      raise wrangler event "subscription" attributes {
        "wellKnown_Tx": wellKnown_eci,
        "Rx_role":"manager",
        "Tx_role":"sensor",
        "Tx_host": host || meta:host
      }
    }
  }

  rule initialize_sensor_profile {
    select when sensor_installer installation_finished
    pre {
      eci = event:attrs{"child_eci"}.klog("eci:")
      name = ent:eci_to_name{eci}.klog("name:")
    }
    if name then
      event:send({
        "eci": eci,
        "domain": "sensor",
        "type": "profile_updated",
        "attrs": {
          "name": name,
          "temperature_threshold": default_threshold,
        }
      })
  }

  rule delete_sensor {
    select when sensor unneeded_sensor
    pre {
      name = event:attrs{"name"}.klog("name:")
      sensor = ent:eci_to_name.filter(function(v,k){ v == name}).klog("sensor:")
      eci = sensor.keys()[0].klog("eci:")
    }
    if eci then
      send_directive("deleting_sensor", {"sensor_name":name})
    fired {
      raise wrangler event "child_deletion_request"
        attributes {"eci": eci};
      clear ent:eci_to_name{eci}
    }
  }

  rule start_report {
    select when sensor_manager report_request
    pre {
      rcn = ent:report_id.klog("rcn:")
      sensors = sensors()
      report_data = {
        "total_sensors": sensors.length(),
        "sensors_responded": 0,
        "temperatures": []
      }.klog("initial report data:")
    }
    fired {
      ent:reports{rcn} := report_data
      raise sensor_manager event "report_initialized"
        attributes {"rcn": rcn, "sensors": sensors}
      ent:report_id := ent:report_id + 1
    }
  }

  rule send_report_requests {
    select when sensor_manager report_initialized
      rcn re#(.*)#
      setting(rcn)
    foreach sensors() setting(sensor)
    pre {
      channel = sensor{"Tx"}.klog("sensor_channel:")
    }
    if channel then
      event:send({
        "eci": channel,
        "domain": "sensor",
        "type": "report_request",
        "attrs": {
          "rcn": rcn
        }
      })
  }

  rule collect_report {
    select when sensor report_created
    rcn re#(.*)#
    setting(rcn)
    pre {
      name = (event:attrs{"sensor"}{"name"}).klog("name:")
      data = (event:attrs{"temp"}.put({"sensor": name})).klog("data:")
      num_responded = (ent:reports{rcn}{"sensors_responded"} + 1).klog("sensors_responded:")
      report_data = (ent:reports{rcn}{"temperatures"}.append(data)).klog("temperatures:")
    }
    always {
      ent:reports{rcn} := ent:reports{rcn}.put(["sensors_responded"], num_responded)
      ent:reports{rcn} := ent:reports{rcn}.put(["temperatures"], report_data)
    }
  }
}
