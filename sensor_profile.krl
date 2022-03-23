ruleset sensor_profile {
  meta {
    name "Sensor Profile"
    author "Tyla Evans"
    provides profile
    shares profile
  }

  global {
    profile = function() {
      {
        "name": ent:name,
        "location": ent:location,
        "temperature_threshold": ent:threshold,
      }
    };
  }

  rule intialization {
    select when wrangler ruleset_installed where event:attrs{"rids"} >< meta:rid
    if true then noop()
    fired {
      ent:name := ent:name.defaultsTo("Wovyn Sensor").klog("initializing name:")
      ent:location := ent:location.defaultsTo("Living Room").klog("initializing location:")
      ent:threshold := ent:threshold.defaultsTo(74).klog("initializing threshold:")
    }
  }

  rule update_name {
    select when sensor profile_updated
      name re#(.+)#
      setting(name)
    if true then noop()
    fired {
      ent:name := name
    }
  }

  rule update_location {
    select when sensor profile_updated
      location re#(.+)#
      setting(location)
    if true then noop()
    fired {
      ent:location := location
    }
  }
  rule update_threshold {
    select when sensor profile_updated
      temperature_threshold re#(\d+[.]?\d*)#
      setting(threshold_string)
    pre {
      threshold = threshold_string.as("Number")
    }
    if true then noop()
    fired {
      ent:threshold := threshold
    }
  }
}
