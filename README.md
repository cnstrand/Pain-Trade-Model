# Pain Trade Model
## This project incorporates several files that work together to create a Pain Trade model. 

### CME Scraper is a job that scrapes information from the CME website and compiles all the open interest volumes for specific commodities into a python dataframe and appends it to a table that exists in SQL. This information is used in the Pain Trade Model.

### Pain Trade is a job that creates a model based on the pain trade assumption by Morgan Stanley. This uses the open interest as an indicator based on the assumption that a certain percentage of contracts will be executed or closed within 30 days of placement. This gives us a better look at what the market includes. 

### The pdfs outline the assumptions of the pain trade model. 
