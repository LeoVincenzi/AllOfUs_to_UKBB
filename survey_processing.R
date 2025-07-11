# The script is going to substitute the conditions present in the survey table. 
# It will be divided between cancer and non-cancer conditions, which will contribute to the final min_data table.
# Moreover, it will create the final min_data_table.

library(dplyr)
library(tidyr)
library(string)
library(data.table)

## First section will regard the creation of columns 20002-*

#Step 1: Loading and preparing files
#Load the survey table
survey_df <- read.csv("survey_table.csv")

# Transform the survey dataframe into long format for easy replacement
survey_long <- survey_df %>%
  pivot_longer(cols = starts_with("20002-"), 
               names_to = "condition_column", 
               values_to = "condition") %>%
  mutate(condition = tolower(condition)) #Convert to lowercase for case-insensitive search

#Load the conversion table downlaoded from UKBioBank with Data-coding 6 codes
conversion_table <- read.csv("cod_6_table.csv")

#Make the conversion table suitable for lexicographical research
conversion_table <- conversion_table %>%
  select(coding, meaning) %>%
  mutate(meaning = tolower(meaning)) %>%
  mutate(coding = as.character(coding))

#Load that home-made conversion table which indicates correspondences for terma in the AllOfUs that does not perfectly match the ones from UKBioBank
mapping_table <- read.csv("DC6_na_conversion.csv", sep="\t", stringsAsFactors = FALSE)

colnames(mapping_table) <- c("condition", "DC6_meaning", "DC6_code")
mapping_table <- mapping_table %>%
  mutate(condition = tolower(condition), DC6_meaning = tolower(DC6_meaning)) 

# Step 2: Search for matches in the first conversion table 
matched_conversion <- survey_long %>%
  left_join(conversion_table, by = c("condition" = "meaning")) %>%
  filter(!is.na(coding)) %>%  # Keep only matching rows 
  mutate(coding = as.character(coding)) %>%  # Convert coding to character 
  select(eid, condition_column, condition, coding)

# Step 3: Identify unmet conditions
unmatched_survey <- survey_long %>%
  anti_join(conversion_table, by = c("condition" = "meaning"))  # Unmatched lines
head(unmatched_survey)

# Step 4: Find matches in the second conversion table (mapping_table)
matched_mapping <- unmatched_survey %>%
  left_join(mapping_table, by = c("condition" = "condition")) %>%
  filter(!is.na(DC6_code)) %>%  # Keep only matching rows
  mutate(coding = as.character(DC6_code)) %>%  # Convert coding to character
  select(eid, condition_column, condition, coding)

# Step 5: Combine the results
final_results <- bind_rows(matched_conversion, matched_mapping) %>%
  arrange(eid, condition_column)

# Step 6: Join survey_long with final_results using condition_column
survey_coded <- survey_long %>%
  left_join(final_results %>% select(eid, condition_column, coding), 
            by = c("eid", "condition_column")) %>%
  mutate(
    # Replace with code if available, otherwise keep the original value
    condition = ifelse(!is.na(coding), coding, condition)
  ) %>%
  select(-coding)  # Remove temporary column coding

# Step 7: Reformat the dataframe to wide format
survey_reformatted <- survey_coded %>%
  pivot_wider(names_from = condition_column, values_from = condition) %>%
  select(-X)  # Remove column X

# Step 8: Salva il risultato in un file CSV
write.csv(survey_reformatted, "survey_reformatted.csv", row.names = FALSE)

##--------------------------------------------------------------------------------------

## Second section will regard the creation of columns 20008-*

# Add an incremental index for each eid
survey_df <- survey_df %>%
  group_by(eid) %>%
  mutate(index = row_number()) %>%
  ungroup()

# Transform to wide format
survey_date <- survey_df %>%
  select(eid, index, survey_date) %>%  # Select only the columns you need
  pivot_wider(
    names_from = index,  # Use the index to create the column names
    values_from = survey_date,  # Values will be dates
    names_prefix = "20008-0."  # Prefix for column names
  )

# Start to shape the final min_data table
final_min_data <- survey_reformatted %>%
  left_join(survey_date, by = "eid")

# Fix the table and remove cancer cells
code_cols <- grep("^20002-", names(final_min_data), value = TRUE)
date_cols <- grep("^20008-", names(final_min_data), value = TRUE)

# Function to clean the table
clean_row <- function(code_row, date_row) {
  keep <- !grepl("cancer", code_row, ignore.case = TRUE) & !is.na(code_row)
  new_codes <- c(code_row[keep], rep(NA, length(code_row) - sum(keep)))
  new_dates <- c(date_row[keep], rep(NA, length(date_row) - sum(keep)))
  c(new_codes, new_dates)
}

# Apply function
cleaned <- t(apply(final_min_data[, c(code_cols, date_cols)], 1, function(row) {
  n_code <- length(code_cols)
  n_date <- length(date_cols)
  code_row <- as.character(row[1:n_code])
  date_row <- as.character(row[(n_code+1):(n_code+n_date)])
  clean_row(code_row, date_row)
}))
    
colnames(cleaned) <- c(code_cols, date_cols)
# Change the column names
final_min_data[, c(code_cols, date_cols)] <- cleaned

## Adding the column 53-0.0 for the date of the survey (it will be the corresponding first column of 20008 dates)
replace_col <- survey_date %>%
  select(eid, "20008-0.1") %>%
  distinct(eid, .keep_all = TRUE) %>%
  rename(`53-0.0` = "20008-0.1")    

final_min_data <- final_min_data %>%
  left_join(replace_col, by = "eid") %>%
  relocate(`53-0.0`, .after = eid)


##--------------------------------------------------------------------------------------

## Third section will regard the creation of columns 40006-*
cancer_cols <- setdiff(names(survey_reformatted), "eid")
# Extract cancer values and create a new dataset
cancer_data <- survey_reformatted %>%
  rowwise() %>%
  mutate(
    cancer_values = list(
      unlist(across(all_of(cancer_cols), ~ ifelse(grepl("cancer", ., ignore.case = TRUE), ., NA)))
    )
  ) %>%
  ungroup() %>%
  select(eid, cancer_values) %>%
  mutate(cancer_values = map(cancer_values, ~ .[!is.na(.)])) %>%
  unnest_wider(cancer_values, names_sep = ".") %>%
  rename_with(~ paste0("40006-0.", seq_along(.)), starts_with("cancer_values"))

# Load a conversion table for the ICD-10 code cancer-related
cancer_table <- fread("cancer_icd10_table.txt")

# Conversion function
convert_to_codes <- function(data, cancer_table) {
  data %>%
    mutate(across(starts_with("40006-0."), ~ {
      map_chr(., ~ cancer_table$Code[match(., cancer_table$Condition)] %||% .)
    }))
}

# Apply conversion
cancer_data_converted <- convert_to_codes(cancer_data, cancer_table)

# Add date columns 40005-* to temp_table
new_cancer_data_converted <- cancer_data_converted %>%
  left_join(final_min_data %>% select(eid, `53-0.0`), by = "eid") %>%
  mutate(across(
      matches("^40006"), 
      ~ ifelse(grepl("^C", .x), as.IDate(`53-0.0`, , format = "%Y-%m-%d"), NA),
      .names = "{.col}_temp"
  )) %>%
  rename_with(~ gsub("40006", "40005", .), matches("_temp$")) %>%
  rename_with(~ gsub("_temp$", "", .), matches("_temp$"))

#Fix the date format in the columns 40005
cols <- grep("^40005", names(new_cancer_data_converted), value = TRUE)
new_cancer_data_converted[cols] <- lapply(new_cancer_data_converted[cols], function(x) as.Date(as.numeric(x), origin = "1970-01-01"))


#Add the cancer table to the final dataframe
final_min_data <- new_cancer_data_converted %>%
  left_join(final_min_data, by = "eid")

final_min_data <- final_min_data %>%
  select(
    eid, 
    matches("^53"), 
    matches("^20002"),  # All column starting with 20002
    matches("^20008"),  # All column starting with 20008
    matches("^40005"),  # All column starting with 40005
    matches("^40006")   # All column starting with 40006
  )
