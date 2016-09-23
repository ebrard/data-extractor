#!/usr/bin/python

"""
Retrieve facebook Ad level insights and account spending for all accounts


"""

from facebookads import FacebookSession
from facebookads import FacebookAdsApi
from facebookads.objects import (
    AdUser,
    AdAccount,
    Campaign
)

from facebookads.adobjects.adsinsights import AdsInsights
from facebookads.adobjects.adreportrun import AdReportRun

import json
import os
import pprint
import csv
from datetime import datetime, date, tzinfo
from pytz import timezone
from dateutil import parser
import time
from dateutil.relativedelta import relativedelta
from pytz import timezone
import argparse

pp = pprint.PrettyPrinter(indent=4)

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description='Facebook data to dwh staging through Facebook insights API')
    arg_parser.add_argument('--accounts', required=False, action='store_true')
    arg_parser.add_argument('--campaigns', required=False, action='store_true')	
    arg_parser.add_argument('--date', required=False, help='Date as YYYY-MM-DD')
    arg_parser.add_argument('--config_file', required=True)
    arg_parser.add_argument('--output_accounts', required=False)
    arg_parser.add_argument('--output_campaigns', required=False)

    cmd_arg = arg_parser.parse_args()  

    config_file = open(cmd_arg.config_file)
    config = json.load(config_file)
    config_file.close()

    ### Check command line arguments
    if cmd_arg.accounts is True and cmd_arg.output_accounts is None:
        print("Accounts option selected without an output file")
        exit(1)

    if cmd_arg.campaigns is True and cmd_arg.output_campaigns is None:
        print("Campaigns option selected without an output file")
        exit(1)

    ### Setup session and api objects
    session = FacebookSession(
    	config['app_id'],
    	config['app_secret'],
    	config['access_token'],
    )

    api = FacebookAdsApi(session)

    ### Create date boundaries
    zurich_time = timezone('Europe/Zurich')

    today = zurich_time.localize(datetime.today().replace(hour=0,minute=0,second=0,microsecond=0))
    tomorrow = today + relativedelta(days=+1)
    yesterday = today + relativedelta(days=-1)
    default_upper_boundary = today + relativedelta(years=+1)
    date_fmt = '%Y-%m-%d'

    ### Set the default query date (yesterday)
    query_date = cmd_arg.date

    if query_date is None:
        query_date = yesterday
        print("Default date: "+query_date.strftime(date_fmt))
    else: 
        query_date = parser.parse(query_date)

    ### Grabing Options
    grab_account  = cmd_arg.accounts
    grab_campaign = cmd_arg.campaigns

	### Open target dump file
    if grab_campaign:
        csvfile = open(cmd_arg.output_campaigns, 'wb')
        to_csv = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    if grab_account:
        file_output_spending = open(cmd_arg.output_accounts, 'wb')
        to_spending = csv.writer(file_output_spending, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)   



    if not grab_campaign and not grab_account:
    	print("At least one command line option is required: --accounts or --campaigns")
    	exit(1)

	### Open Facebook session
    FacebookAdsApi.set_default_api(api)

    ### Facebook objects fields list
    account_fields  = [AdAccount.Field.name]
    
    campaign_fields = [Campaign.Field.name, 
    	Campaign.Field.effective_status,
    	Campaign.Field.status,
    	Campaign.Field.configured_status,
    	Campaign.Field.start_time,
    	Campaign.Field.stop_time]

    query_insights_params = params = {'level': AdsInsights.Level.ad,'fields': ['account_name','adset_name','ad_name','spend', 'clicks'], 
        'time_range': {
        'since': query_date.strftime(date_fmt),
        'until': query_date.strftime(date_fmt)
    },
    'filtering':[{'field':"ad.impressions",'operator':"GREATER_THAN",'value':0}]
    }

    query_accounts_params = {'level': AdsInsights.Level.account,'fields': ['account_name','spend'], 
        'time_range': {
        'since': query_date.strftime(date_fmt),
        'until': query_date.strftime(date_fmt)
    }}

    # This endpoint does not support querying deleted
    campaign_parameters = {Campaign.Field.effective_status:[Campaign.Status.archived,
    															Campaign.Status.paused,
    															Campaign.Status.active],
    															'time_range': {
       			 												'since': query_date.strftime(date_fmt),
        														'until': query_date.strftime(date_fmt),
    }} 

    ### Setup user and read the object from the server
    me = AdUser(fbid='me')

    ### Get all accounts connected to the user
    my_accounts = me.get_ad_accounts(fields=account_fields)

    ### List of campaigns that need to be grabed
    campaigns_list = list()

    print "Integration data for date %s" % (query_date.strftime(date_fmt))

    if grab_campaign:
    	print("Creating campaigns list")
    for account in my_accounts:

    	if grab_account:
    		print("Retrieving account %s spending") % (account.get('name','Error'))
    		async_job = account.get_insights(params=query_accounts_params, async=True) 
    		time.sleep(1)
    		async_job.remote_read()
    		time.sleep(1)

    		while async_job[AdReportRun.Field.async_percent_completion] < 100:
    			time.sleep(1)
    			async_job.remote_read()
    		time.sleep(1)

    		query_result = async_job.get_result()

    		if query_result:
    			query_result = query_result[0]
    			to_spending.writerow(
    				(
    					query_date.strftime(date_fmt),
    					account['name'],
    					query_result['spend']
    				)
    			)

    	if grab_campaign:

	    	for campaign in account.get_campaigns(fields=campaign_fields, params=campaign_parameters):
	    		campaign_start = campaign.get('start_time')
	    		campaign_end   = campaign.get('stop_time')
	
	    		### We first check is the dates are set
	    		if campaign_start is None:
	    			campaign_start = default_upper_boundary
	    		else:
	    			campaign_start = parser.parse(campaign_start)
	
	    		if campaign_end is None:
	    			campaign_end = default_upper_boundary
	    		else:
	    			campaign_end = parser.parse(campaign_end)
	
	    		if ( query_date >= campaign_start and query_date <= campaign_end ): 
	    			if 'YYMMDD' not in campaign.get('name'): ### Remove YYMMDD_ pattern matching campaign
	    				if (campaign_end > zurich_time.localize(datetime(2014,12,31)) and campaign_start > zurich_time.localize(datetime(2000,01,01)) ):
			    			campaigns_list.append(campaign)


    if grab_campaign:

	    print("Querying campaigns edge")
	    ### Iterate over campaigns list
	    for campaign in campaigns_list:
	   		print "Retrieving campaign %s" % (campaign.get('name','unknown'))
	   		### We need to check if the current date if within the campaign period
	   		campaign_start = campaign.get('start_time')
	   		campaign_end   = campaign.get('stop_time')
	
	   		### We first check is the dates are set
	   		if campaign_start is None:
	   			campaign_start = default_upper_boundary
	   		else:
	   			campaign_start = parser.parse(campaign_start)
	   		if campaign_end is None:
	   			campaign_end = default_upper_boundary
	   		else:
	   			campaign_end = parser.parse(campaign_end)
	   		if ( query_date >= campaign_start and query_date <= campaign_end ):
	   			### Campaign is active at requested date
	   			async_job = campaign.get_insights(params=params, async=True) 
	   			### Initial query of remote job
	   			time.sleep(1)
	   			async_job.remote_read()
	   			# Wait for batch job to complete
	   			while async_job[AdReportRun.Field.async_percent_completion] < 100:
	   				# Replace this by a decaying retry
	   				time.sleep(1)
	   				### Query of remote job
	   				async_job.remote_read()
	   			time.sleep(1)	
	   			### Retrieve query result as a json list
	   			query_result = async_job.get_result()
	   			if query_result:
	   				for ad in query_result:
	
	   					to_csv.writerow((
	   					query_date.strftime(date_fmt),
		   				ad['account_name'],
	   					campaign['id'], 
	   					campaign['name'].encode('utf-8',errors='ignore').replace('\n', ' ').replace('\r', ''),
	   					campaign['effective_status'],
	   					campaign['status'],
	   					campaign['start_time'],
		   				campaign.get('stop_time',default_upper_boundary),
		   				ad['adset_name'].encode('utf-8',errors='ignore').replace('\n', ' ').replace('\r', ''),
		   				ad['ad_name'].encode('utf-8',errors='ignore').replace('\n', ' ').replace('\r', ''),
		   				ad['clicks'],
		   				ad['spend']
	   					))
	
	   		else:
	   			print("Campaign out of range")

