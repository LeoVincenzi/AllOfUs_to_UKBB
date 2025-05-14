##Estraction of Person.csv

library(tidyverse)
library(bigrquery)

# This query represents dataset for domain "person" and was generated for All of Us Registered Tier Dataset v8
dataset_PheWAS_person_sql <- paste("
    SELECT
        person.person_id,
        NULL AS age_at_assessment_centre,
        p_gender_concept.concept_name AS sex,
        person.month_of_birth AS birth_month,
        person.year_of_birth AS birth_year,
        p_sex_at_birth_concept.concept_name AS genetic_sex,
        NULL AS missingness
    FROM
        `person` person 
    LEFT JOIN
        `concept` p_gender_concept 
            ON person.gender_concept_id = p_gender_concept.concept_id
    LEFT JOIN
        `concept` p_sex_at_birth_concept 
            ON person.sex_at_birth_concept_id = p_sex_at_birth_concept.concept_id
    WHERE
        person.PERSON_ID IN (SELECT
            distinct person_id  
        FROM
            `cb_search_person` cb_search_person  
        WHERE
            cb_search_person.person_id IN (SELECT
                person_id 
            FROM
                `person` p 
            WHERE
                ethnicity_concept_id IN (38003564) ) 
            AND cb_search_person.person_id IN (SELECT
                person_id 
            FROM
                `person` p 
            WHERE
                race_concept_id IN (8527, 8516, 2000000008, 8515) ) 
            AND cb_search_person.person_id IN (SELECT
                person_id 
            FROM
                `cb_search_person` p 
            WHERE
                has_ehr_data = 1 ) )", sep="")

# Formulate a Cloud Storage destination path for the data exported from BigQuery.
# NOTE: By default data exported multiple times on the same day will overwrite older copies.
#       But data exported on a different days will write to a new location so that historical
#       copies can be kept as the dataset definition is changed.
PheWAS_person_path <- file.path(
  Sys.getenv("WORKSPACE_BUCKET"),
  "bq_exports",
  Sys.getenv("OWNER_EMAIL"),
  strftime(lubridate::now(), "%Y%m%d"),  # Comment out this line if you want the export to always overwrite.
  "PheWAS",
  "PheWAS_person.csv")
message(str_glue('The data will be written to {PheWAS_person_path}. Use this path when reading ',
                 'the data into your notebooks in the future.'))

# Perform the query and export the dataset to Cloud Storage as CSV files.
# NOTE: You only need to run `bq_table_save` once. After that, you can
#       just read data from the CSVs in Cloud Storage.
bq_table_save(
  bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_PheWAS_person_sql, billing = Sys.getenv("GOOGLE_PROJECT")),
  PheWAS_person_path,
  destination_format = "CSV")



# Read the data directly from Cloud Storage into memory.
# NOTE: Alternatively you can `gsutil -m cp {person_46948194_path}` to copy these files
#       to the Jupyter disk.
read_bq_export_from_workspace_bucket <- function(export_path) {
  col_types <- cols(gender = col_character(), race = col_character(), ethnicity = col_character(), sex_at_birth = col_character(), self_reported_category = col_character())
  bind_rows(
    map(system2('gsutil', args = c('ls', export_path), stdout = TRUE, stderr = TRUE),
        function(csv) {
          message(str_glue('Loading {csv}.'))
          chunk <- read_csv(pipe(str_glue('gsutil cat {csv}')), col_types = col_types, show_col_types = FALSE)
          if (is.null(col_types)) {
            col_types <- spec(chunk)
          }
          chunk
        }))
}
PheeWAS_person_df <- read_bq_export_from_workspace_bucket(PheWAS_person_path)

##Extraction of GP_clinical table
ibrary(tidyverse)
library(bigrquery)
library(lubridate)
library(glue)

#Configure the parameters
project_id <- Sys.getenv("GOOGLE_PROJECT")  # ID del progetto BigQuery
dataset_query <- Sys.getenv("WORKSPACE_CDR")  # Dataset di BigQuery
my_bucket <- Sys.getenv('WORKSPACE_BUCKET')
chunk_size <- 100000  # Numero di righe per chunk

eid_list <- sort(read.csv("PheWAS_person.csv")$person_id)

start_value=8816483
eid_list <- eid_list[eid_list >= start_value]

# Split the list in subgroups of 10 eid
group_size <- 1000
eid_groups <- split(eid_list, ceiling(seq_along(eid_list) / group_size))

# Iterate over chunks
c=272
for (group in eid_groups) {
    
    eid_filter <- paste(group, collapse = ", ")
  # Query to extract a chunk
  chunk_query <- paste0("
    SELECT
        co.person_id AS eid,
        NULL AS data_provider,
        co.condition_start_date AS event_date,
        NULL AS read_2,
        c.concept_code AS read_3
    FROM
        condition_occurrence co
    LEFT JOIN
        concept_relationship cr 
            ON co.condition_concept_id = cr.concept_id_1 
            AND cr.relationship_id = 'Mapped from'
    LEFT JOIN
        concept c 
            ON cr.concept_id_2 = c.concept_id 
            AND c.vocabulary_id IN ('SNOMED') 
            AND c.domain_id IN ('Condition')
    WHERE
        c.concept_code IS NOT NULL
        AND co.person_id IN (", eid_filter, ")
    ORDER BY
        eid, event_date")

  # Execute the query for the chunk
  hesin_df <- bq_table_download(bq_dataset_query(dataset_query, chunk_query, billing = project_id))

  # Save the chunk to a CSV file
  output_file <- paste0("gp_clinical_", c, ".csv")
  write.csv(as.data.frame(hesin_df), file = output_file, row.names = FALSE)
  system(paste0("gsutil -m cp ", output_file," ", my_bucket), intern=T)
  
  # Print the status
  cat("Salvato i dati per il gruppo", c, "in", output_file, "\n")
    c<-c+1
}

# Read and concatenate the csv files
file_list <- list.files(pattern = "gp_clinical_.*\\.csv", full.names = TRUE)
all_data <- do.call(rbind, lapply(file_list, read.csv))

# Save the final CSV
write.csv(all_data, file = "gp_clinical.csv", row.names = FALSE)

##HESIN
library(tidyverse)
library(bigrquery)
library(lubridate)
library(glue)

project_id <- Sys.getenv("GOOGLE_PROJECT")  # ID del progetto BigQuery
dataset_query <- Sys.getenv("WORKSPACE_CDR")  # Dataset di BigQuery
chunk_size <- 100000  # Numero di righe per chunk

eid_list <- sort(read.csv("PheWAS_person.csv")$person_id)

#start_value=5564262
#eid_list <- eid_list[eid_list >= start_value]

group_size <- 1000
eid_groups <- split(eid_list, ceiling(seq_along(eid_list) / group_size))
head(eid_list)
head(eid_groups)

# Iterate over chunks
c=0
for (group in eid_groups) {
    
    eid_filter <- paste(group, collapse = ", ")
  # Query to extract a chunk
  chunk_query <- paste0("
    SELECT
        CONCAT(v.person_id, '-', 
            DENSE_RANK() OVER (PARTITION BY v.person_id ORDER BY v.visit_start_date)) AS dnx_hesin_id,
        v.person_id AS eid,
        ROW_NUMBER() OVER (PARTITION BY v.person_id ORDER BY v.visit_start_date) AS ins_index,
        v.visit_start_date AS epistart,
        v.visit_end_date AS epiend,
        v.visit_start_date AS admidate,
        v.visit_end_date AS disdate,
        DATE_DIFF(v.visit_end_date, v.visit_start_date, DAY) AS speldur,
        NULL AS epidur
    FROM
        visit_occurrence v
    WHERE
        v.person_id IN (", eid_filter, ")
    ORDER BY
        eid, ins_index")

  # Execute the query for the chunk
  hesin_df <- bq_table_download(bq_dataset_query(dataset_query, chunk_query, billing = project_id))

  # Save the chunk to a CSV file
  output_file <- paste0("hesin_", c, ".csv")
  write.csv(as.data.frame(hesin_df), file = output_file, row.names = FALSE)
  
  # Print the status
  cat("Salvato i dati per il gruppo", c, "in", output_file, "\n")
    c<-c+1
}



# Leggi tutti i file CSV e concatenali
file_list <- list.files(pattern = "hesin_.*\\.csv", full.names = TRUE)
all_data <- do.call(rbind, lapply(file_list, read.csv))

# Salva il file concatenato
write.csv(all_data, file = "hesin_combined.csv", row.names = FALSE)

##HESIN_DIAG (Cancer diagnosis)
library(tidyverse)
library(bigrquery)
library(lubridate)
library(glue)

# This query represents dataset "PheeWAS_db" for domain "person" and was generated for All of Us Registered Tier Dataset v8
dataset_PheeWAS_hesin_sql <- paste("
    SELECT
        v.person_id AS eid,
        DENSE_RANK() OVER (PARTITION BY v.person_id ORDER BY v.visit_start_date) AS ins_index,
        ROW_NUMBER() OVER (PARTITION BY v.person_id, v.visit_start_date ORDER BY co.condition_start_date) AS arr_index,
        NULL AS level,
        NULL AS diag_icd9,
        NULL AS diag_icd9_nb,
        c.concept_code AS icd10_code,
        NULL AS diag_icd10_nb
    FROM
        visit_occurrence v
    LEFT JOIN
        condition_occurrence co ON v.person_id = co.person_id AND v.visit_occurrence_id = co.visit_occurrence_id
    LEFT JOIN
        concept_relationship cr ON co.condition_concept_id = cr.concept_id_1 AND cr.relationship_id='Mapped from'
    LEFT JOIN
        concept c on cr.concept_id_2 = c.concept_id AND c.vocabulary_id IN ('ICD10CM')
    WHERE
        c.concept_code IS NOT NULL
        AND (c.concept_code LIKE 'C%' OR c.concept_code LIKE 'D0%')
        AND v.person_id IN (
            SELECT DISTINCT cb.person_id
            FROM cb_search_person cb
            INNER JOIN person p 
                ON cb.person_id = p.person_id
            WHERE
                p.ethnicity_concept_id = 38003564
                AND p.race_concept_id IN (8527, 8516, 2000000008, 8515)
                AND cb.has_ehr_data = 1
        )
    ORDER BY
        eid, ins_index, arr_index", sep="")

hesin_diag_c_df <- bq_table_download(bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_PheeWAS_hesin_sql, billing=Sys.getenv("GOOGLE_PROJECT")))


dim(hesin_diag_c_df)
head(hesin_diag_c_df, 20)

write.csv(as.data.frame(hesin_diag_c_df), file="hesin_diag_cancer.csv")

##HESIN_DIAG (no cancer)
library(tidyverse)
library(bigrquery)
library(lubridate)
library(glue)

project_id <- Sys.getenv("GOOGLE_PROJECT")  # ID del progetto BigQuery
dataset_query <- Sys.getenv("WORKSPACE_CDR")  # Dataset di BigQuery
chunk_size <- 100000  # Numero di righe per chunk

eid_list <- sort(read.csv("PheWAS_person.csv")$person_id)

start_value=6857471
eid_list <- eid_list[eid_list >= start_value]

group_size <- 1000
eid_groups <- split(eid_list, ceiling(seq_along(eid_list) / group_size))
head(eid_list)

# Iterate over chunks
c=246
for (group in eid_groups) {
    
    eid_filter <- paste(group, collapse = ", ")
  # Query to extract a chunk
  chunk_query <- paste0("
    SELECT
        v.person_id AS eid,
        DENSE_RANK() OVER (PARTITION BY v.person_id ORDER BY v.visit_start_date) AS ins_index,
        ROW_NUMBER() OVER (PARTITION BY v.person_id, v.visit_start_date ORDER BY co.condition_start_date) AS arr_index,
        NULL AS level,
        NULL AS diag_icd9,
        NULL AS diag_icd9_nb,
        c.concept_code AS icd10_code,
        NULL AS diag_icd10_nb
    FROM
        visit_occurrence v
    LEFT JOIN
        condition_occurrence co ON v.person_id = co.person_id AND v.visit_occurrence_id = co.visit_occurrence_id
    LEFT JOIN
        concept_relationship cr ON co.condition_concept_id = cr.concept_id_1 AND cr.relationship_id='Mapped from'
    LEFT JOIN
        concept c on cr.concept_id_2 = c.concept_id AND c.vocabulary_id IN ('ICD10CM')
    WHERE
        c.concept_code IS NOT NULL
        AND (c.concept_code NOT LIKE 'C%' OR c.concept_code NOT LIKE 'D0%')
        AND v.person_id IN (", eid_filter, ")
    ORDER BY
        eid, ins_index, arr_index")

  # Execute the query for the chunk
  hesin_df <- bq_table_download(bq_dataset_query(dataset_query, chunk_query, billing = project_id))

  # Save the chunk to a CSV file
  output_file <- paste0("hesin_NC_", c, ".csv")
  write.csv(as.data.frame(hesin_df), file = output_file, row.names = FALSE)
  
  # Print the status
  cat("Salvato i dati per il gruppo", c, "in", output_file, "\n")
    c<-c+1
}

# Create a list of the files' names
file_list <- list.files(pattern = "hesin_NC_.*\\.csv", full.names = TRUE)
# Define the dimension of the blocks
block_size <- 10

# Divide the blocks
file_blocks <- split(file_list, ceiling(seq_along(file_list) / block_size))

# Load and save one blocks
for (i in seq_along(file_blocks)) {
  block_files <- file_blocks[[i]]
  
  # Read and concatenate
  block_data <- do.call(rbind, lapply(block_files, read.csv))
  
  # Save the temporary block in an intermediate file
  write.csv(block_data, file = paste0("block_", i, ".csv"), row.names = FALSE)
  
  cat("Block", i, "saved.\n")
}

# Merge the intermediate files
intermediate_files <- list.files(pattern = "block_.*\\.csv", full.names = TRUE)
final_data <- do.call(rbind, lapply(intermediate_files, read.csv))

write.csv(final_data, file = "hesin_diag_no_cancer.csv", row.names = FALSE)




##HESIN_OPER
library(tidyverse)
library(bigrquery)
library(lubridate)
library(glue)

# Configura i parametri
project_id <- Sys.getenv("GOOGLE_PROJECT")  # ID del progetto BigQuery
dataset_query <- Sys.getenv("WORKSPACE_CDR")  # Dataset di BigQuery
chunk_size <- 100000  # Numero di righe per chunk

eid_list <- sort(read.csv("PheWAS_person.csv")$person_id)

start_value=5564262
eid_list <- eid_list[eid_list >= start_value]

# Dividi la lista in gruppi di 10 identificativi
group_size <- 1000
eid_groups <- split(eid_list, ceiling(seq_along(eid_list) / group_size))
head(eid_list)
head(eid_groups)



# Iterate over chunks
c=0
for (group in eid_groups) {
    
    eid_filter <- paste(group, collapse = ", ")
  # Query to extract a chunk
  chunk_query <- paste0("
    SELECT
        CONCAT(p.person_id, '-', 
            DENSE_RANK() OVER (PARTITION BY v.person_id ORDER BY v.visit_start_date), '-', 
            ROW_NUMBER() OVER (PARTITION BY p.person_id, v.visit_start_date ORDER BY p.procedure_date)) AS dnx_hesin_oper_id,
        CONCAT(p.person_id, '-', 
            DENSE_RANK() OVER (PARTITION BY v.person_id ORDER BY v.visit_start_date)) AS dnx_hesin_id,
        p.person_id AS eid,
        DENSE_RANK() OVER (PARTITION BY v.person_id ORDER BY v.visit_start_date) AS ins_index,
        DATE_DIFF(p.procedure_date, v.visit_start_date, DAY) AS preopdur,
        ROW_NUMBER() OVER (PARTITION BY p.person_id, v.visit_start_date ORDER BY p.procedure_date) AS arr_index,
        NULL AS level,
        p.procedure_date AS opdate,
        NULL AS oper3,
        NULL AS oper3_nb,
        c.concept_code AS oper4,
        NULL AS oper4_nb,
        DATE_DIFF(v.visit_end_date, p.procedure_date, DAY) AS posopdur
    FROM
        procedure_occurrence p
    LEFT JOIN
        visit_occurrence v ON p.person_id = v.person_id AND p.visit_occurrence_id = v.visit_occurrence_id
    LEFT JOIN
        concept c ON p.procedure_concept_id = c.concept_id AND c.domain_id = 'Procedure' AND c.vocabulary_id = 'OPCS4'
    WHERE
        p.procedure_date BETWEEN v.visit_start_date AND v.visit_end_date
        AND p.person_id IN (", eid_filter, ")
    ORDER BY
        eid, ins_index, arr_index")

  # Execute the query for the chunk
  hesin_df <- bq_table_download(bq_dataset_query(dataset_query, chunk_query, billing = project_id))

  # Save the chunk to a CSV file
  output_file <- paste0("hesin_oper_", c, ".csv")
  write.csv(as.data.frame(hesin_df), file = output_file, row.names = FALSE)
  
  # Print the status
  cat("Salvato i dati per il gruppo", c, "in", output_file, "\n")
    c<-c+1
}



# Leggi tutti i file CSV e concatenali
file_list <- list.files(pattern = "hesin_oper.*\\.csv", full.names = TRUE)
all_data <- do.call(rbind, lapply(file_list, read.csv))

# Salva il file concatenato
write.csv(all_data, file = "hesin_oper_combined.csv", row.names = FALSE)

#DEATH_DATES
library(tidyverse)
library(bigrquery)
library(lubridate)
library(glue)

# This query represents dataset "PheeWAS_db" for domain "person" and was generated for All of Us Registered Tier Dataset v8
dataset_PheeWAS_gp_script_sql <- paste("
    SELECT
        d.person_id AS eid,
        DENSE_RANK() OVER (PARTITION BY d.person_id ORDER BY d.death_date) AS ins_index,
        c.concept_name AS source,
        c.concept_code AS d_source,
        d.death_date
    FROM
        death d
    LEFT JOIN
        concept_relationship cr ON d.death_type_concept_id = cr.concept_id_1 AND cr.relationship_id = 'Mapped from'
    LEFT JOIN
        concept c ON cr.concept_id_2 = c.concept_id
        AND d.person_id IN (
            SELECT DISTINCT cb.person_id
            FROM cb_search_person cb
            INNER JOIN person p 
                ON cb.person_id = p.person_id
            WHERE
                p.ethnicity_concept_id = 38003564
                AND p.race_concept_id IN (8527, 8516, 2000000008, 8515)
                AND cb.has_ehr_data = 1
        )
    ORDER BY
        eid, ins_index", sep="")

death_df <- bq_table_download(bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_PheeWAS_gp_script_sql, billing=Sys.getenv("GOOGLE_PROJECT")))

write.csv(as.data.frame(death_df), file="death.csv")

