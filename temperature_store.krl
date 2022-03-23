ruleset temperature_store {
  meta {
    name "Temperature Store"
    author "Tyla Evans"
    provides temperatures, threshold_violations, inrange_temperatures
    shares temperatures, threshold_violations, inrange_temperatures
    use module sensor_profile alias profile
    use module io.picolabs.subscription alias subs
  }

  global {
    temperatures = function() {
      ent:temperatures.defaultsTo([])
    };
    threshold_violations = function() {
      ent:threshold_violations.defaultsTo([])
    };
    inrange_temperatures = function() {
      ent:temperatures.defaultsTo([]).filter(
        function(temp) {
          ent:threshold_violations.defaultsTo([]).none(
            function(violation) {
              temp{"temp"} == violation{"temp"}
            })
        })
    };
  }

  rule clear_temperatures {
    select when sensor reading_reset
    if true then noop()
    always {
      ent:temperatures := [].klog("reset temperatures:")
      ent:threshold_violations := [].klog("reset temperature violations:")
    }
  }

  rule collect_temperatures {
    select when wovyn new_temperature_reading
    pre {
      temperature = event:attrs{"temperature"}.klog("temperature:")
      timestamp = event:attrs{"timestamp"}.klog("timestamp:")
    }
    if true then noop()
    always {
      ent:temperatures := ent:temperatures.defaultsTo([], "initialization was needed").klog("current temperatures:");
      ent:temperatures := ent:temperatures.append({"temp": temperature, "time": timestamp}).klog("new temperatures:");
    }
  }

  rule collect_threshold_violation {
    select when wovyn threshold_violation
    pre {
      temperature = event:attrs{"temperature"}.klog("temperature:")
      timestamp = event:attrs{"timestamp"}.klog("timestamp:")
    }
    if true then noop()
    always {
      ent:threshold_violations := ent:threshold_violations.defaultsTo([], "initialization was needed").klog("current temperature violations:");
      ent:threshold_violations := ent:threshold_violations.append({"temp": temperature, "time": timestamp}).klog("new temperature violations:");
    }
  }

  rule send_recent_temperature {
    select when sensor report_request
      rcn re#(.*)#
      setting(rcn)
    pre {
      latest_temperature = (ent:temperatures.reverse().head()).klog("latest temp:")
      profile = profile:profile().klog("profile:")
      Rx_channel = event:eci.klog("Rx:")
      Tx_channel = (subs:established().filter(
        function(sub){
          sub{"Rx"} == Rx_channel
        }
      )[0]{"Tx"}).klog("Tx:")
    }
    event:send({"eci":Tx_channel, "domain":"sensor", "type":"report_created", "attrs":event:attrs.put({"sensor": profile, "temp": latest_temperature})})
  }
}
