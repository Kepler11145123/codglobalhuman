Feature: Feature for KbtqGlSpkOutCodatakbtqcddeconinformati
  Scenario: Run a service process
    Given The id of the process as 'KbtqGlSpkOutCodatakbtqcddeconinformati'
    And The env 'INPUT_PATH' targeting the resource file 'data/olympics.csv'
    And The env 'INPUT_SCHEMA_PATH' targeting the resource file 'schema/inputSchema.json'
    And The env 'OUTPUT_PATH' targeting the target file 'output'
    And The env 'OUTPUT_SCHEMA_PATH' targeting the resource file 'schema/outputSchema.json'
    And A config file with the contents:
      """
      config {
          inputs = [
              {
                  name = "olympics"
                  fullpath = ${?INPUT_PATH}
                  uri_schema = ${?INPUT_SCHEMA_PATH}
              }
          ]

          outputs = [
              {
                  name = "olympicsFiltered"
                  fullpath = ${?OUTPUT_PATH}
                  uri_schema = ${?OUTPUT_SCHEMA_PATH}
              }
          ]
      }
      """
    When Executing the Launcher
    Then The exit code should be 0
