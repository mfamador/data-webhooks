Feature: Health
  In order to deploy this service into a high available cluster
  As a Site Reliability Engineer
  I need to query a health check endpoint on my service

  Scenario: Service is up and ready
    When I query the health endpoint
    Then I should have status code 200
    When I query the readiness endpoint
    Then I should have status code 200
