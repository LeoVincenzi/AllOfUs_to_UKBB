if (!requireNamespace("R.utils", quietly = TRUE)) {
  install.packages("R.utils")
}

library(dplyr)
library(bit64)
library(data.table)

#Preperation of the GP table and conversion table
gpclinical_df <- fread("gp_clinical.csv.gz")
#Input file like:
#eid	data_provider	event_date	read_2	read_3
#<int>	<lgl>	<IDate>	<lgl>	<int64>
#1000000	NA	2012-04-15	NA	111479008
#1000000	NA	2012-04-15	NA	192076004
#1000000	NA	2012-04-15	NA	192203002
#1000000	NA	2012-04-15	NA	231457005
#1000000	NA	2012-04-15	NA	36474008
#1000000	NA	2012-04-15	NA	397791000000109

# Read the conversion table
conv_table <- read.table("CTV3_to_SNOMED_table.txt", sep="\t", header=TRUE)
# Convert the read3 column to character
conv_table$SCT_CONCEPTID <- as.character(conv_table$SCT_CONCEPTID)
# Remove duplicates
unique_conv_table <- conv_table %>%
  distinct() %>%
  select(CTV3_CONCEPTID, SCT_CONCEPTID)
#Prepare the table
unique_conv_table <- unique(unique_conv_table, by = "SCT_CONCEPTID")   
setDT(unique_conv_table)

# Filter only SNOMED codes that are present in the conversion table
filtered_gpclinical <- gpclinical_df[read_3 %in% unique_conv_table$SCT_CONCEPTID]
setDT(filtered_gpclinical)
# Convert read_3 to character
filtered_gpclinical[, read_3 := as.character(read_3)]

# Get a list of the eids
unique_eids <- unique(filtered_gpclinical$eid)

#Iterative conversion
block_size <- 1000  # Number of eid per block
eid_blocks <- split(unique(filtered_gpclinical$eid), ceiling(seq_along(unique(filtered_gpclinical$eid)) / block_size))

results <- list()
block_index<-1

# Iretate for each block
for (block in eid_blocks) {
  # Filter data for the current block
  block_data <- filtered_gpclinical[eid %in% block]
  block_data <- unique(block_data, by = "read_3")

  # Join the tables
  block_ctv3 <- block_data[unique_conv_table, on = .(read_3 = SCT_CONCEPTID), nomatch = 0] 
  block_ctv3 <- block_data[block_ctv3, read_3 := i.CTV3_CONCEPTID, on = .(eid, read_3)]
    
  # Save a block as CSV
  file_name <- sprintf("block_ctv3_%02d.csv", block_index)  # (z.B. block_ctv3_01.csv)
  fwrite(block_ctv3, file_name)
  
  # Stampa il progresso
  cat("Processed block with ", length(block), "eid. Saved in", file_name, "\n")
  # Augment the index
  block_index <- block_index + 1
}
cat("Processing completed. All blocks have been saved.\n")

# Combine the results in a single dataframe
final_ctv3_table <- rbindlist(lapply(sprintf("block_ctv3_%02d.csv", seq_len(block_index - 1)), fread))

#You should get to something like: 
#eid	data_provider	event_date	read_2	read_3
#<int>	<lgl>	<IDate>	<lgl>	<chr>
#1000000	NA	2012-04-15	NA	XE1ZD
#1000000	NA	2012-04-15	NA	X00Rc
#1000000	NA	2012-04-15	NA	XaCHo
#1000000	NA	2012-04-15	NA	Eu02y
#1000000	NA	2012-04-15	NA	Eu06.
#1000000	NA	2012-04-15	NA	Eu041

# Identify missing rows (eid missing from final_ctv3_table)
missing_rows <- gpclinical_df[!eid %in% filtered_gpclinical$eid]
missing_rows <- missing_rows[, read_3 := NA_character_]
missing_rows <- missing_rows[, read_3 := as.character(read_3)]
missing_rows <- missing_rows[!duplicated(missing_rows, by = c("eid", "event_date"))]

# Combine missing rows with the final table
complete_table <- rbindlist(list(final_ctv3_table, missing_rows), fill = TRUE)

# Save the result in a CSV file
write.csv(complete_table, "gp_clinical_CTV3.csv", row.names = FALSE)







