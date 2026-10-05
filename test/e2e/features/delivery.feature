Feature: Delivering notifications
  Notifications are rendered from a template, queued on Kafka and handed to the
  channel's provider. Transient provider failures are retried with backoff;
  permanent ones fail fast.

  Background:
    Given the provider stubs are reset

  Scenario: An SMS is delivered and confirmed by the provider's webhook
    When I send an "otp-sms" notification to "+15550100001" with data:
      | code    | 482913 |
      | minutes | 10     |
    Then the response status is 202
    And within 30 seconds the notification status is "DELIVERED"
    And the "sms" provider received the body "Your Acme verification code is 482913. It expires in 10 minutes."

  Scenario: A push notification is delivered
    When I send an "order-shipped-push" notification to "device-token-abc" with data:
      | orderId | A-1001   |
      | eta     | Thursday |
    Then within 30 seconds the notification status is "DELIVERED"

  Scenario: Transient failures are retried until the provider recovers
    Given the "sms" provider fails the next 2 requests
    When I send an "otp-sms" notification to "+15550100002" with data:
      | code    | 111111 |
      | minutes | 5      |
    Then within 60 seconds the notification status is "DELIVERED"
    And the notification has 3 delivery attempts
    And attempt 1 has outcome "RETRYABLE_ERROR" and error code "HTTP_503"

  Scenario: A permanent provider rejection is not retried
    Given the "sms" provider rejects every request
    When I send an "otp-sms" notification to "+15550100003" with data:
      | code    | 222222 |
      | minutes | 5      |
    Then within 30 seconds the notification status is "FAILED"
    And the notification has 1 delivery attempt

  Scenario: A template variable is missing
    When I send an "otp-sms" notification to "+15550100004" with data:
      | code | 333333 |
    Then the response status is 422
    And the error code is "TEMPLATE_RENDER"
