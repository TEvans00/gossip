ruleset com.twilio.sdk {
  meta {
    configure using
      authToken = ""
      sessionID = ""
    provides sendMessage, messages
  }
  global {
    base_url = "https://api.twilio.com/2010-04-01"

    sendMessage = defaction(toNum, fromNum, body) {
      reqBody = {
        "To": toNum,
        "From": fromNum,
        "Body": body
      }
      reqAuth = {
        "username": sessionID,
        "password": authToken
      }
      http:post(<<#{base_url}/Accounts/#{sessionID}/Messages.json>>,
        auth=reqAuth, form=reqBody) setting(response)
      return response.klog("response: ")
    }

    messages = function(toNum, fromNum, pageSize, page, pageToken) {
      queryString = {
        "To": toNum || null,
        "From": fromNum || null,
        "PageSize": pageSize || null,
        "page": page || null,
        "PageToken": pageToken || null
      }
      reqAuth = {
        "username": sessionID,
        "password": authToken
      }
      response = http:get(<<#{base_url}/Accounts/#{sessionID}/Messages.json>>,
        auth=reqAuth, qs=queryString)
      response{"content"}.decode()
    }
  }
}
