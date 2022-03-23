ruleset wovyn_base {
  meta {
    name "Wovyn Base"
    author "Tyla Evans"
    use module sensor_profile
    use module io.picolabs.subscription alias subs
  }

  global {
    getProfile = function() {
      sensor_profile:profile
    }
    managers = function() {
      subs:established().filter(
        function(sub){
          sub{"Tx_role"} == "manager"
        }
      )
    }
    default_temperature_threshold = 74
  }

  rule process_heartbeat {
    select when wovyn heartbeat
    if event:attrs{"genericThing"} then
      send_directive("heartbeat", {"body": "Temperature reading received"})
    fired {
      raise wovyn event "new_temperature_reading" attributes {
        "temperature" : event:attrs{"genericThing"}{"data"}{"temperature"}[0]{"temperatureF"},
        "timestamp" : event:time
      }
    }
  }

  rule find_high_temps {
    select when wovyn new_temperature_reading
    pre {
      temperature = event:attrs{"temperature"}.klog("temperature:")
      profile = getProfile()
      threshold = profile{"temperature_threshold"}.defaultsTo(default_temperature_threshold).klog("threshold:")
    }
    if true then noop()
    always {
      raise wovyn event "threshold_violation"
      attributes event:attrs
      if (temperature > threshold).klog("above threshold:")
    }
  }

  rule threshold_notification {
    select when wovyn threshold_violation
    foreach managers() setting (manager)
    pre {
      profile = getProfile()
      threshold = profile{"temperature_threshold"}.defaultsTo(default_temperature_threshold).klog("threshold:")
      name = profile{"name"}.klog("name")
      eci = manager{"Tx"}.klog("eci:")
      host = (manager{"Tx_host"}.defaultsTo(meta:host)).klog("host:")
    }
    if eci then event:send(
        { "eci": eci,
          "eid": "threshold-violation",
          "domain": "sensor",
          "type": "threshold_violation",
          "attrs": {
            "temperature": event:attrs{"temperature"},
            "time": event:attrs{"time"},
            "threshold": threshold,
            "name": name
          }
        }, host
      )
  }
}
