Feature: Processing
  In order to deploy Readings.Digestor service
  As a Data Engineer
  I need to process the incoming messages from Readings topic

  Scenario: Readings Sunny day
    Given 1 valid message with deviceID 1 and 1 sample
    When The message is sent to readings topic
    Then I should have 1 record in Readings table
    Then I should have 1 record in LastValues table
    And I should have 0 messages in dead-letter queue
    And I should have 0 messages in readings-retries topic

  Scenario: Readings with multiple samples
    Given 1 valid message with deviceID 1 and 13 samples
    When The message is sent to readings topic
    Then I should have 13 records in Readings table
    Then I should have 13 records in LastValues table
    Then I should have 2 records in BatteryData table
    Then I should have 2 records in NetworkQuality table
    Then I should have 1 record in VolumeReadings table
    And I should have 0 messages in dead-letter queue
    And I should have 0 messages in readings-retries topic

  Scenario: Readings with fatal errors are sent to the Dead-Letter Queue
    Given 1 invalid message with deviceID 1 and 1 sample
    When The message is sent to readings topic
    Then I should have 0 records in Readings table
    And I should have 1 message in dead-letter queue
    And I should have 0 messages in readings-retries topic

  Scenario: Readings with transient errors are sent to the Retry Queue, and if all retries fail to Dead-Letter
    Given 1 valid message with nonexistent deviceID 10000 and 1 sample
    When The message is sent to readings topic
    Then I should have 0 records in Readings table
    And I should have 1 message in readings-retries topic
    When The message is retried for the last time
    Then I should have 1 message in dead-letter queue
