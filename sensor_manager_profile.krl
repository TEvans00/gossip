ruleset sensor_manager_profile {
  meta {
    name "Sensor Manager Profile"
    description <<
      Stores profile information for a sensor manager
    >>
    author "Tyla Evans"
    use module com.twilio.sdk alias sdk
      with
        authToken = meta:rulesetConfig{"auth_token"}
        sessionID = meta:rulesetConfig{"session_id"}
  }

  global {
    notification_number = "+13033324277"
  }

  rule trigger_threshold_notification {
    select when sensor threshold_violation
    pre {
      body = ("Warning: Sensor " + event:attrs{"name"} + " has detected a temperature of " + event:attrs{"temperature"} + " degrees, which is above the threshold of " + event:attrs{"threshold"} + " degrees.").klog("temperature warning message: ")
    }
    if true then sdk:sendMessage(notification_number, "+16066033227", body) setting(response)
  }

}
