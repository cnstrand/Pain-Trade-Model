# Pain Trade Model
### This project incorporates several files that work together to create a Pain Trade model. 

### CMEScraper.py is a job that scrapes information from the CME website and compiles all the open interest volumes for specific commodities into a python dataframe and appends it to a table that exists in SQL. 

### PainTrade.py is a job that creates a model based on the pain trade assumption by Morgan Stanley. This uses the open interest as an indicator based on the assumption that a certain percentage of contracts will be executed or closed within 30 days of placement. This gives us a better look at what the market includes. 

### The pdfs outline the assumptions of the pain trade model pertaining to options trading in the white paper published by Morgan Stanley.
