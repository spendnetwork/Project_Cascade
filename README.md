# Project_Cascade_Ita

This repository takes two sets of Italian organisation data and links them together using dedupe.io's csvlink and csvdedupe modules. Given the limitations of quality assessment regarding matches, additional calculations take place to recycle the resulting training data in order to improve both the quantity and quality of the matches.

The first set of data will contain poor quality, manually curated data, which probably contains many variations of the same organisation data.

The second set contains publicly available information on all registered Italian organisations. 

The purpose is to find as many possible matches between the two datasets, verifying the quality of the manually curated (aka "private") data.

Should the process be stopped at any point, the module has been constructed to check for each file created as it progresses. Therefore you can end the module and resume from where you left off.

## Using the Module
Modification of the module in its current state will be required depending on the datasets used.

### Initial Set Up & Familiarisation

1. Clone the repo:
```
git clone https://github.com/DMells/Project_Cascade_Ita
```

2. Install dedupe:
```
git clone "https://github.com/DMells/csvdedupe"
```

3. Install with pipenv:
```
pipenv install
```

OR

Install with virtualenv:

```
virtualenv venv -p python3
source venv/bin/activate
pip install -r requirements.txt
```

4. Place the two raw data files into Raw_Data/

5. Run the module
```
cd Project_Cascade_Ita
python Project_Cascade.py --pub_raw_name 'public_data_sample.csv'
```
Or rename the sample file to public_data.csv, which is the default name used when calling 
```
python Project_Cascade.py
```

The module makes use of argument parsing, with the following arguments:
```
--priv_raw_name (default: 'private_data.csv')
--pub_raw_name (default: 'public_data.csv')
--priv_adj_name (default: 'priv_data_adj.csv')
--pub_adj_name (default: 'pub_data_adj.csv')
--recycle (No arguments)
```

To amend the names if needed :
```
python Project_Cascade.py --priv_raw_name <filename>  # ...etc
```

The `--recycle` flag is used once the module has been run and trained for the first time. When this flag is used, the module will run for a second time, but will incorporate the training data obtained from the manual matching process created in the first 'round', as  kick-start of sorts. See 5. Recycling Matches below for more info.

The default file formats are :

##### Private Data
Filename : private_data.csv
Fields : 'id' (int), 'supplier_name' (str), 'supplier_streetadd' (str)

##### Public Data
Filename : public_data.csv
Fields : 'org_name', 'street_address1', 'street_address2', 'street_address3', 'Org_ID'

## General Processes

### Data Cleaning
1. The module takes both datafiles and creates a new column with the org name cleaned up to help the matching process. For example, all company type suffixes (ltd, llp, srl etc) are standardised.
2. In the same new column, punctuation is removed.
3. For the public data set which has several address columns, these are all merged into one, with related row entries duplicated.  

### Deduplication
4.  The dedupe module comes into play next in two stages. First is the matching phase, which joins together our manual private data to the public data. It does this using [dedupe](https://github.com/dedupeio/csvdedupe)'s csvlink command. Training data has been provided to provide the best quality matches, and so you are required to do nothing here, however if you want to modify the training data just use the `training` flag when calling the module:
```
python project_cascade.py --training
```
It is recommended that you study the dedupe documentation before modifying the training data, as experimentation is required to prevent over or under-fitting of the matching process. I have provided some notes at the end of this readme to explain my methods.
5. Second, the module takes this matched data and assigns rows into groups called clusters, in the event that two rows within the private data actually refer to the same company. This outputs both a cluster_ID and a confidence score of how likely that row belongs to that cluster.

### Further Data Manipulation
6. We now have our matched and clustered dataset, which means that our private data is now linked to registry/public data for verification and it is also grouped into clusters so we don't eg: contact the same company twice.
If any data **within a cluster** hasn't been matched to public data , then if there is a match anywhere within that cluster, that public data is applied to the rest of the cluster. This is to increase the number of absolute matches. Quality control comes next.

### Extracting the best matches
7. Because of the many different ways of writing a company name, dedupe's matching/clustering phase can only take us so far.
8. We then introduce a Levenshtein distance ratio to the data, which indicates just how good each match is based on the amount of alteration it would require to make one string the same as the other. The higher the score the better.
9. Note that a short string with a difference of 1 letter between it and another string is much less likely to be a match than a long string with the same difference. Based on this, and a pre-defined config file, we introduce a cascading quality filter, which decreases the minimum levenshtein distance we are willing to accept, as the string length increases.
You can add as many config files as you like to experiment with different combinations of this quality control filter system. 
10. A short stats file is created so the user can see high level results of the different config files. 

### Re-cycling the matches
11. With the best matches filtered for, the user can then pick the best config file (by reviewing the stats file in `Outputs/<process_type>/Matches_Stats_x.csv`), and then will be prompted by the terminal to go through and manually verify the quality of each match.
12. These quality matches, and poor quality matches, are then converted to a json training file, which can then be re-fed back into dedupe as a kick-start to more accurate training but now including the street address field. Once the process has completed for the first time, re-run the module using the `recycle` flag:

```
python project_cascade.py --recycle
```

This will run the entire process again but will use the new training data and will attempt to match both the organisation name and the address.

### Training Notes
#### 1. Matching
The best way to train the matching data is to have a strategy of sorts prior to entering the dedupe phase. The best approach I found was to use the 'Unsure' option liberally. For example, if the two strings are the same but one is clearly an abbreviated version of the other, hit 'Unsure'. Each decision will impact the rest of the training data and therefore the outcome of the matches. Dedupe cannot apply context to the data, and so being strict prevents any 'rules' being learnt that make no sense.

#### 2. Clustering
Clustering here is important because it will affect how much of the matched data will be copied to unmatched, but closely related private data. 

#### 3. Altering the config files
Once the training has complete, this is where the config files come into play, as we are now attempting to extract the best quality matches factoring in that longer strings can have lower levenshtein ratios and still be good matches compared to shorter strings.

Create a new config file as required (must follow the current naming convention) and experiment with the 'char_counts' and 'min_match_score', making sure to increase one as you decrease the other. The module will automatically register additional files and run the process and output the stats to separate csvs for you to compare. These files will be saved in `Outputs/Extracted_Matches`.
