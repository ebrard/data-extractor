# Facebook data extractor

Simple Python script to extract raw data from Facebook based on the python [wrapper sdk](https://github.com/facebook/facebook-python-ads-sdk) 
for the [Facebook Ads insights API](https://developers.facebook.com/docs/marketing-api/insights-api).

Illustrate how to retrieve account metrics (here spending) and ads level insights.

## Usage 

### Accounts level spendings

Account spendings are extracted to a headerless csv file.

```bash
extract_facebook.py.py --accounts --output_accounts spending.csv --config_file config.json --date 2016-09-22
```

### Ad level insights

Ads insights are extracted to a JSON file (ad name is the key).

```bash
extract_facebook.py --date 2016-09-04 --config_file config.json --campaigns --output_campaigns insights_2016-09-04.json
```
