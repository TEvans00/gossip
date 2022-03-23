ruleset sensor_installer {
  meta {
    name "Sensor Installer"
    description <<
      Installs the necessary rulesets to turn a pico into a temperature sensor representation
    >>
    author "Tyla Evans"
    use module io.picolabs.wrangler alias wrangler
    use module io.picolabs.subscription alias subs
  }

  global {
    rulesets = {
      "sensor_profile": {
        "config": {}
      },
      "temperature_store": {
        "config": {}
      },
      "wovyn_base": {
        "config": {}
      },
      "io.picolabs.wovyn.emitter": {
        "config": {}
      },
      "sensor_subscriptions": {
        "config": {}
      },
      "gossip": {
        "config": {}
      }
    }

    install_request = defaction(rid){
      installed_rulesets = wrangler:installedRIDs()
      exists = installed_rulesets.any(function(id){id ==rid}).klog("rule already installed:")
      if not exists then
        event:send({
          "eci": meta:eci.klog("eci:"),
          "domain": "wrangler",
          "type": "install_ruleset_request",
          "attrs": {
            "absoluteURL": meta:rulesetURI,
            "rid": rid.klog("rid:"),
            "config": (rulesets{rid}{"config"}).klog("config:"),
          }
        })
    }
  }

  rule create_sensor_channel {
    select when wrangler ruleset_installed where event:attrs{"rids"} >< meta:rid
    pre {
      tags = ["sensor"]
      eventPolicy = {"allow": [{"domain": "*", "name": "*"}], "deny": []}
      queryPolicy = {"allow": [{"rid": "*", "name": "*"}], "deny": []}
    }
    if true then wrangler:createChannel(tags,eventPolicy,queryPolicy) setting(channel)
    always {
      ent:sensor_eci := channel{"id"}.klog("sensor eci:")
      raise sensor_installer event "install_request"
    }
  }

  rule trigger_installation_on_self {
    select when sensor_installer install_request
    pre {
      self_channel = wrangler:channels("system,self")
      self_eci = self_channel.head().get("id")
    }
    if self_eci then
      event:send({
        "eci": self_eci,
        "domain": "sensor_installer",
        "type": "install_request_on_self"
      })
  }

  rule install_rulesets {
    select when sensor_installer install_request_on_self
    foreach rulesets.keys() setting(rid)
      install_request(rid)
  }

  rule report_installation_finished {
    select when wrangler ruleset_installed where event:attrs{"rids"}.any(function(rid){rulesets >< rid})
    pre {
      rids = rulesets.keys()
      installed_rulesets = wrangler:installedRIDs()
      installation_finished = rids.all(function(rid) {installed_rulesets >< rid}).klog("installation finished:")
      parent_eci = wrangler:parent_eci()
      child_channel = wrangler:channels("system,child")
      child_eci = child_channel.head().get("id")
      sensor_eci = ent:sensor_eci
      wellKnown_eci = subs:wellKnown_Rx(){"id"}.klog("wellKnown_eci:")
    }
    if installation_finished && parent_eci then
      event:send({
        "eci": parent_eci,
        "domain": "sensor_installer",
        "type": "installation_finished",
        "attrs": {
          "child_eci": child_eci,
          "sensor_eci": sensor_eci,
          "wellKnown_eci": wellKnown_eci
        }
      })
    fired {
      raise sensor_installer event "execution_finished"
    }
  }

  rule cleanup {
    select when sensor_installer execution_finished
    event:send({
      "eci": meta:eci,
      "domain": "wrangler",
      "type": "uninstall_ruleset_request",
      "attrs": {
        "rid": "sensor_installer"
      }
    })
  }
}
