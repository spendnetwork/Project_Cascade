# UK Buyers Manual Matching - Operational Guide

This guide provides instructions for running the matching script using WebApps.

 Within the storage browser, navigate to /home/snbot/davidm/Project_Cascade/Regions/Outputs/Manual_Matches
 
 The csv contained in this folder consists of recently obtained UK buyer data, which has been matched to registry data.
 
 The objective is to verify whether or not the matches are correct or not.
 
 #### CSV columns to consider
 
 `src_name` is the externally obtained company name. We want to match this source data to more official registry data
 
 `reg_name` is the registry name that the src_name has been matched to.
 
 `leven_dist_n` is an indication of the quality of the match of the names - the higher the better. If a 100 is present, then the manual match which have automatically been confirmed
 
 `manual_match_n` in this column enter 'Y', 'N' or 'U' (unsure) if the two names relate to the same company
 
 `src_address_adj` the address which we want to verify
 
 `reg_address_adj` self explanatory
 
 `manual_match_na` following the same convention above, decide whether both the name AND the address match.
 
 ### Once checking is complete
 
 - In the FTP browser drag and drop the file into the Uploads folder
 
 - Upload the file to the database by running `python runfile.py --upload`