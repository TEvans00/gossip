ruleset sensor_subscriptions {
  meta {
    name "Sensor Subscriptions"
    description <<
      Manages the subscription requests a sensor gets
    >>
    author "Tyla Evans"
  }

  rule process_subscription_request {
    select when wrangler inbound_pending_subscription_added
    always {
      raise wrangler event "pending_subscription_approval"
        attributes event:attrs
      ent:subscriptionTx := event:attrs{"Tx"}
    }
  }
}
