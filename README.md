# ioo-bmce-captech project
## Scripts
The order of using the scripts is (some modifications are due in the main scripts to make it include all weeks and years, but the functions used remain the same) : 
- `process.py` to process the dates of the articles into the same format. The `processed_data.json` file contains the processed articles that are the output of this step, that can directly be imported into the mongodb
- `bigram.py` to extract the bigrams and compute their TFIDF Score.
- `sentences.py` to get opinion scores on the sentences of the bigrams.
- `reduction.py` to reduce the data by deleting all the data with a weak TFIDF mean.
- `features.py` to compute the different scores (persistence and anomaly).
- `postprocess`.py to finalize the format of the data that is needed in the front end. `full_data.json` is the output result of this step for 20 weeks.
- `aggregate.py` is the aggregation of the data in a format to link it finally with the front end.


## Database
The titles of the collections needed can be found in the beginning of its scripts.


## Front End 
The `app.js` file here will have to replace the same file in the front end code, with the data in the localhost mongodb database. 

