This readme concerns the sources of the model inputs text files:

File name convention = 'Classification-source'.txt

ALL source names must not contain hyphens (-) : change these for underscores so the model can make the distinction between classification and source file name.'

i.e. 

`Government-uk_data.public-bodies`

must be changed to
 
`Government-uk_data.public_bodies`

##### CIC or Charity

- DrKane data - Community Interest Companies.txt
- DrKane data - Registered Charities.txt (combined with above)

##### Further Education

- DrKane data - Further Education.txt

##### Government

- DrKane data - Government.txt
- uk_data.entity table (level1 like Central 'Government')
- uk_data.government-organisations
- uk_data.public-bodies

##### Local Authority

- DrKane data - Local Authority.txt
- uk_data.entity table (level1 like 'Local Government') (combined txt file)
- uk_data.entity table (level1 like 'Fire') (combined txt file)
- uk_data.entity table (level1 like 'Justice') (combined txt file
- uk_data.sct-local-authorities
- uk_data.wls-local-authorities
- uk_data.nir-local-authorities
- uk_data.england-local-authorities

##### Parish or Town Council 

- DrKane data - Parish or Town Council.txt

##### Royal Charter Company

- DrKane data - Royal Charter Company.txt

##### School

- DrKane data - School.txt

##### University

- DrKane data - University.txt

##### Limited Company

- DrKane data - Private Limited Company.txt
- DrKane data - Company Limited By Guarantee.txt (combined with above)
- DrKane data - Registered Society.txt

##### NHS

- uk_data.entity (level1 like 'NHS')
- uk_data.clinical-commissioning-groups
 
##### Devolved Government

- uk_data.entity (level1 like 'Devolved Government')

##### PLC

-ocds.orgs_lookup (contains PLC)


##### LLP

-ocds.orgs_lookup (contains LLP)

### Adjustments

- moved '%University%' from Royal Charter (drkane) to University (drkane)
- "%college%" moved from Royal Charter (dk)) to Further Education (drkane)
- '%college%' moved from School dk to Further Education dk
- '%academy%' moved from School dk to Further Education dk
- Moved drkane registered society under category 'Limited Company'

### Exluded Sources:

SOURCE: uk_data.entity - 'level1 like 'PUBLIC CORPORATIONS''
REASON: many rows belong to government, 59 strings total

SOURCE: uk_data.entity - 'level1 like 'Public Companies'
REASON: only 1 string




### To re-fit the updated data to the neural network:

In the command line, run `python generate_model.py --input-dir './updated_model_inputs'`

If you don't to overwrite a previously trained model (default:`model.pkl.gz`) add `--output-file` argument and specify the new filename for the model

