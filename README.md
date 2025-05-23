# AllOfUs_to_UKBB
## How to convert the OMOP tables from AllOfUs to the UKBB format

This repository is meant to enable you to recover data from the AllOfUs Research Program dataset and prepare it in the format of the UKBioBank, in order to be able to perform analysis on the two datasets.
All the scripts reported are intended to be run inside the AllOfUs Notebook.

## Table extraction

In the `Table_extraction.R` script, you can find the extraction for each table with the relative SQL queries.
The tables we are going to download are the following:
- Person_data.csv;
- GP_clinical table;
- Hesin_table;
- Hesin_diag_cancer table (this is relevant just for statistical purposes);
- Hesin_diag_non_cancer table;
- Hesin_operation table;
- Death table.
The death_cause and the Gp_drug tables were not included.

Considering that some of the tables were extracted through an iterative seeding approach, to manage high dimensions, as for the hesin_diag_non_cancer.
NB: In this case, the suggestion is to work with high resources: 16 CPU and at least 60 GB of RAM.

## Survey table extraction
A separate task was for the survey since the AllOfUs provides a specific code for the surveys we want to evaluate.
Follow the `Survey_extraction.R` script in this case. Remember that the outcome from the script needs to be integrated with the "PheWAS_person.csv" from the other script to set the min_data.csv table is needed for further analysis.


## Survey post-processing
Once the survey outcome has been obtained, we need to build the min_data table. The script `survey_processing.R` goes through a series of transformations which apply the Data-Coding-6 system (https://biobank.ctsu.ox.ac.uk/crystal/coding.cgi?id=6) of the UKBioBank to the symptoms extracted from the Questionnaire in the AllOfUs. Both table cod_6_table.csv and DC6_na_conversion.csv contribute to performing the conversion of the terms. Moreover, the script separates the self-reported DC6 terms from the self-reported cancer terms (which do not have the DC6 as they are only for non-cancer conditions), which are further converted into their ICD10 codes values using the cancer_icd10_table.txt table. The final output will present 4 columns:
- 20002: columns with the self-reported terms in DC6 coding;
- 20008: columns with the corresponding date of the self-reported conditions;
- 40005: columns with the self-reported cancer terms in ICD10 coding;
- 40006: columns with the corresponding date of the self-reported cancer.
This table will need to be integrated with information from the Person_data.csv.

## GP conversion
The GP_clinical table needs a post-processing since the SNOMED code in the read_3 columns needs to be converted into CTV3 codes. The script `GP_clinical_processing.R`, through the mapping table CTV3_to_SNOMED_table.xlsx, makes this conversion.
