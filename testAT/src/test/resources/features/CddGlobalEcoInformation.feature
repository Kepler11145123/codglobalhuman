@cddGlobalEcoInformation
Feature: Feature for cddGlobalEcoInformation

  Scenario: Test cddGlobalEcoInformation should result OK
    Given The id of the process as cddGlobalEcoInformation
    And a config file in path src/test/resources/config/cddGlobalEcoInformation.conf
    When Executing the Launcher
    Then exit code is equal to 0
    Given a dataframe located at path src/test/resources/data/cddGlobalEcoInformation/output/ with alias dfOutput and config:
        | type   |
        | parquet |
    When I read dfOutput as dataframe
    Given a dataframe located at path src/test/resources/data/cddGlobalEcoInformation/expected/ with alias dfExpected and config:
        | type   |
        | parquet |
    When I read dfExpected as dataframe
    Then dfOutput dataframe has the same records than dfExpected dataframe