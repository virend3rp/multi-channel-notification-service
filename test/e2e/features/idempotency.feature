Feature: Idempotency
  A client retrying a request with the same Idempotency-Key must not cause a
  second message to reach the user.

  Background:
    Given the provider stubs are reset

  Scenario: Given the same Idempotency-Key, only one message is sent
    Given a new idempotency key
    When I send an "otp-sms" notification to "+15550300001" with the idempotency key and data:
      | code    | 700001 |
      | minutes | 5      |
    And I send the same request again
    Then the response status is 200
    And both responses refer to the same notification
    And within 30 seconds the notification status is "DELIVERED"
    And the "sms" provider accepted exactly 1 message for the notification

  Scenario: Reusing an Idempotency-Key with a different payload is rejected
    Given a new idempotency key
    When I send an "otp-sms" notification to "+15550300002" with the idempotency key and data:
      | code    | 700002 |
      | minutes | 5      |
    And I send an "otp-sms" notification to "+15550300002" with the idempotency key and data:
      | code    | 999999 |
      | minutes | 5      |
    Then the response status is 422
    And the error code is "IDEMPOTENCY_KEY_REUSED"
