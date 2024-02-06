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

## Configutarion files descrition

- cddGlobalClearingHouse.conf: This file contains the input and output configuration for the execution of the table t_xdkq_clearing_house
  - LAST_DAY_MONTH = Last Calendar day of current month (2022-03-31)
  - LAST_DAY_MONTH_PREV =  Last Calendar day of the previous month (2022-02-28)
  - ODATE = current date (D-1)(2023-06-30)

- cddGlobalCustomer.conf: This file contains the input and output configuration for the execution of the table t_kbtq_eom_customer
  - YEAR_PREV = current year (2019)
  - MONTH_PREV =  current month (12)
  - LAST_DAY_MONTH = Last Calendar day of current month (2019-12-31)
  - LAST_DAY_MONTH_PREV =  Last Calendar day of the previous month (2019-11-29)
  - ODATE = current date (D-1)(2023-06-30)

- cddGlobalEcoInformation.conf: This file contains the input and output configuration for the execution of the table t_kbtq_eom_econ_information
  - YEAR_PREV = current year (2019)
  - MONTH_PREV =  current month (12)
  - LAST_DAY_MONTH = Last Calendar day of current month (2019-12-31)
  - LAST_DAY_MONTH_PREV =  Last Calendar day of the previous month (2019-11-29)
  - ODATE = current date (D-1)(2023-06-30)

- cddGlobalEomLocalGroup.conf: This file contains the input and output configuration for the execution of the table t_ksag_eom_local_group
  - YEAR = current year (2019)
  - MOTNH = current month (12)
  - DAY = current day (31)
  - YEAR_PREV = current year (2019)
  - MONTH_PREV =  current month (12)
  - LAST_DAY_MONTH = Last Calendar day of current month (2019-12-31)
  - ODATE = current date (D-1)(2023-06-30)

- cddGlobalEomRecoding.conf: This file contains the input and output configuration for the execution of the table t_kbtq_eom_recoding
  - LAST_BUSINESS_DAY_MONTH = Last business day of the current month (2019-12-31)
  - LAST_DAY_MONTH = Last Calendar day of current month (2019-12-31)
  - ODATE = current date (D-1)(2023-06-30)

- cddGlobalEomSectorization.conf: This file contains the input and output configuration for the execution of the table t_ksag_eom_sectorization
  - YEAR = current year (2020)
  - MOTNH = current month (01)
  - DAY = current day (01)
  - YEAR_PREV = current year (2020)
  - MONTH_PREV =  current month (01)
  - LAST_BUSINESS_DAY_MONTH = Last business day of the current month (2020-01-01)
  - LAST_DAY_MONTH = Last Calendar day of current month (2020-01-31)
  - ODATE = current date (D-1)(2023-06-30)

- cddGlobalEomSegmentation.conf: This file contains the input and output configuration for the execution of the table t_ksag_eom_segmentation
  - YEAR_PREV = current year (2020)
  - MONTH_PREV =  current month (12)
  - LAST_BUSINESS_DAY_MONTH = Last business day of the current month (2020-12-31)
  - LAST_DAY_MONTH = Last Calendar day of current month (2020-12-31)
  - ODATE = current date (D-1)(2023-06-30)

- cddLocalEomSectorization.conf: This file contains the input and output configuration for the execution of the table t_csag_ml_eom_sectorization
  - YEAR = current year (2020)
  - MOTNH = current month (01)
  - DAY = current day (01)
  - YEAR_PREV = current year (2020)
  - MONTH_PREV =  current month (01)
  - LAST_BUSINESS_DAY_MONTH = Last business day of the current month (2020-01-01)
  - LAST_DAY_MONTH = Last Calendar day of current month (2020-01-31)
  - ODATE = current date (D-1)(2023-06-30)

- cddpeopletPrtcptLclGroup.conf: This file contains the input and output configuration for the execution of the table t_ksag_eom_prtcpt_lcl_group
  - YEAR_PREV = current year (2020)
  - MONTH_PREV =  current month (12)
  - LAST_BUSINESS_DAY_MONTH = Last business day of the current month (2020-12-31)
  - LAST_DAY_MONTH = Last Calendar day of current month (2020-12-31)
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