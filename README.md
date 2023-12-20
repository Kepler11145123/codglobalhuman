# Configuration files for KBTQ and project cddpeopledataproc. 

# General description
Basic information about individuals and their contact information, as well as information about the source of non-customer information.

### Owners

For any bugs or questions, please reach out to ["james.parreno@bbva.com" , "pablosantiago.bustos@bbva.com"]

## First Execution.

Go to your IDE run configurations window and set the next configuration:
* Enable the maven profile `run-local`
* Set the next VM Option =>  `-Dspark.master=local[*]`
* Set the main class => `com.bbva.datioamproduct.CddGlobalModuleLauncher`
* Set as program argument a valid path to a configuration file (not empty)
* Set environment variables INPUT_PATH and OUTPUT_PATH for process external files

## Configutarion files description

- cddGlobalEcoInformation.conf: This file contains the input and output configuration for the execution of the table t_kbtq_eom_econ_information
  - YEAR_PREV = current year (2019)
  - MONTH_PREV =  current month (12)
  - LAST_DAY_MONTH = Last Calendar day of current month (2019-12-31)
  - LAST_DAY_MONTH_PREV =  Last Calendar day of the previous month (2019-11-29)
  - ODATE = current date (D-1)(2023-06-30)

## ${confsType} version
- ${confsType} version: x.y.z

## Framework version
- spark version: 3.1
- spark version: 2.2.1 (Jobs CTLs)

## More information
[comment]: <> (here we can provide more information, such as guides, good practices, information about the team...)
If you have any uncertainties about the use of Git or the repository structure, feel free to check out this links: [Git Regulations](https://docs.google.com/document/d/1cZIZAp4yOrlgLSfxiYO_-KGCFF9j81YJQ3JuN9bXU2s/edit?usp=drive_link) and [Regulations for configuration and job repositories](https://docs.google.com/document/d/1LtVTevmqJ3g1Bd8PltkLkAQSq5L84g1Jj-5A1XnonDs/edit?usp=drive_link)
branching_model: gitflow