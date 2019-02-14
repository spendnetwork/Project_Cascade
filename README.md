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

2. Install with pipenv:
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

3. Install dedupe repo:
```
git clone "https://github.com/DMells/csvdedupe"
```

4. Place the two raw data files into Raw_Data/

5. Run the module
```
cd Project_Cascade_Ita
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

### 1.  Data Cleaning
- Standardisation of company suffixes (i.e. ltd vs limited for UK companies)
- Irrelevant punctuation removed
- Multiple address columns in the public data merged into one single column, with the records duplicated. This is to help the learning/training process.

### 2. Deduplication
- First the module links the two datasets together, to join our manual private data to more official sources. It does this using [dedupe](https://github.com/dedupeio/csvdedupe)'s csvlink command.
- Second, the module takes this matched data and assigns rows to clusters, in the event that two rows actually refer to the same company. This outputs both a cluster_ID and a confidence score of how likely that row belongs to that cluster. Clustering is done using dedupe's csvdedupe command.
- Training files for both clustering and matching are provided already. Note that the first 'round' of processing only uses one field, the organisation name. The second 'round' will include the address as well, 

### 3. Further Data Manipulation
- If any data within the cluster hasn't been matched to a public source, then if there is a match anywhere within that cluster, that public data is applied to the rest of the cluster.

### 4. Extracting the best matches
- Because of the many different ways of writing a company name, dedupe's matching/clustering phase can only take us so far.
- We then introduce a Levenshtein distance ratio to the data, which indicates just how good each match is.
- A short string with a difference of 1 letter is much less likely to be a match than a long string with the same difference.
- Based on this, and a pre-defined config file, we introduce a cascading quality filter, which decreases the minimum levenshtein distance we are willing to accept, as the string length increases.
- For each configuration file, a separate short stats csv is created so the user can tweak the config files and retain some indication of which one produces the most/best quality matches.

### 5. Re-cycling the matches
- With the best matches obtained, the user can then pick the best config file (by reviewing the stats file), and then will be prompted to go through and manually verify the quality of each match. 
- These quality matches, and poor quality matches, are then converted to a json training file, which can then be re-fed back into dedupe as a kick-start to more accurate training but now including the street address field.

### Training Conventions
#### 1. Matching
The best way to train the matching data is to have a strategy of sorts prior to entering the dedupe phase. The best approach I found was to use the 'Unsure' option liberally. For example, if the two strings are the same but one is clearly an abbreviated version of the other, hit 'Unsure'. Each decision will impact the rest of the training data and therefore the outcome of the matches. Dedupe cannot apply context to the data, and so being strict prevents any 'rules' being learnt that make no sense.

#### 2. Clustering
Clustering here is important because it will affect how much of the matched data will be copied to unmatched, but closely related private data. 

#### 3. Altering the config files
Once the training has complete, this is where the config files come into play, as we are now attempting to extract the best quality matches factoring in that longer strings can have lower levenshtein ratios and still be good matches compared to shorter strings.

Create a new config file as required (must follow the current naming convention) and experiment with the 'char_counts' and 'min_match_score', making sure to increase one as you decrease the other. The module will automatically register additional files and run the process and output the stats to separate csvs for you to compare. These files will be saved in `Outputs/Extracted_Matches`.






