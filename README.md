# AllOfUs_to_UKBB
## How to convert the OMOP tables from AllOfUs to the UKBB format

This repository is meant to enable you to recover data from the AllOfUs Research Program dataset and prepare it in the format of the UKBioBank, in order to be able to perform analysis on the two datasets.
All the scripts reported are intended to be run inside the AllOfUs Notebook.

## Table extraction

In the `Table_extraction.R` script, you can find the extraction for each table with the relative SQL queries.
The tables we are going to download are the following:
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
Follow the `Survey_extraction.R` script in this case. Remember that the outcome from the script needs to be integrated with the "PheWAS_person.csv" from the other script to set the min_data.cav table needed for further analysis.
