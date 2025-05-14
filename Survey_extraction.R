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
  mutate(time_index = row_number()) %>%  # TIme related index (increasing for each event on the same date)
  ungroup()

# Create the codnition table (Columns 20002 of the min_data final table)
survey_h <- survey %>%
  mutate(condition_column = paste0("20002-", date_index, ".", time_index)) %>%  # Create columns' names
  select(eid, condition_column, condition) %>% 
  pivot_wider(names_from = condition_column, values_from = condition)  # Transform to the wide format

#We saved this table for the conversion of it's terms in DC6 encoding
write.csv(as.data.frame(survey_h), file="condition_survey.csv")

# Create the time table (Columns 20008 of the min_data final table)
# NB: This table will always have the same date for each condition
survey_date <- survey %>%
  mutate(condition_column = paste0("20008-", date_index, ".", time_index)) %>%  # Create columns' names
  select(eid, condition_column, survey_date) %>% 
  pivot_wider(names_from = condition_column, values_from = survey_date)  # Transform to the wide format

# Preparing the 53-0.0 columns
replace_col <- survey_date %>%
  select(eid, "20008-0.1")
names(replace_col)[names(replace_col) == "20008-0.1"] <- "datesurvey"
replace_col <- replace_col %>%
  distinct(eid, .keep_all = TRUE)

# Join with the big table
survey_date <- survey_date %>%
  left_join(replace_col, by = "eid")

# Reloate survey_date column
survey_date <- survey_date %>%
  relocate(datesurvey, .after = eid) %>%  
  rename(`53-0.0` = datesurvey)   

write.csv(as.data.frame(survey_h), file="survey_table.csv")

# The obtained table need to be integrated with the PheWAS_person.csv obtain in the Table_extraction.R to get the final version of min_data.csv
