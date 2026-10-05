Feature: Dead-letter queue
  When a provider keeps failing, a notification is retried on the 1s, 5s and 30s
  retry topics. After the final attempt it is dead-lettered, and an operator can
  replay it from the admin API.

  Background:
    Given the provider stubs are reset

  Scenario: Given the SMS provider keeps failing, the notification lands in the DLQ
    Given the "sms" provider fails the next 4 requests
    When I send an "otp-sms" notification to "+15550200001" with data:
      | code    | 900001 |
      | minutes | 5      |
    Then within 90 seconds the notification status is "DEAD_LETTERED"
    And the notification has 4 delivery attempts
    And the DLQ contains the notification

  Scenario: A dead-lettered notification is replayed and delivered
    Given the "sms" provider fails the next 4 requests
    And I send an "otp-sms" notification to "+15550200002" with data:
      | code    | 900002 |
      | minutes | 5      |
    And within 90 seconds the notification status is "DEAD_LETTERED"
    When I replay the notification from the DLQ
    Then within 30 seconds the notification status is "DELIVERED"
    And the DLQ entry is marked as replayed
