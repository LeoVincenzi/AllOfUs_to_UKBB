#This script wants to obtain information about the self-reported conditions and self-reported cancers
library(tidyverse)
library(bigrquery)
library(tidyr)

#Extraction of the data
dataset_53060926_survey_sql <- paste("
    SELECT
        person_id AS eid,
        REGEXP_REPLACE(question, '^.*for ', '') AS condition,
        survey_datetime
    FROM
        ds_survey
    WHERE
        answer LIKE '%Yes%' AND survey = 'Personal and Family Health History' AND question LIKE '%currently%'
    ORDER BY
        person_id, survey_datetime", sep="")

survey<-bq_table_download(bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_53060926_survey_sql, billing = Sys.getenv("GOOGLE_PROJECT")))
survey$condition <- gsub("\\?", "", survey$condition)

# Add date and time related indexes
survey <- survey %>%
  mutate(survey_date = as.Date(survey_datetime),  # Extract the date
         survey_time = format(as.POSIXct(survey_datetime), "%H:%M:%S")) %>%  # Extract the time
  group_by(eid) %>%
  mutate(date_index = dense_rank(survey_date) - 1) %>%  # Date related index (same index for the same date)
  ungroup() %>%
  group_by(eid, survey_date) %>%
  mutate(time_index = row_number()) %>%  # Time related index (increasing for each event on the same date)
  ungroup()

write.csv(as.data.frame(survey_h), file="survey_table.csv")

# The obtained table need to be integrated with the PheWAS_person.csv obtain in the Table_extraction.R to get the final version of min_data.csv
