#from py_functions import rightmove_scrape as rm
#import importlib
#importlib.reload(rm)
# !/usr/bin/env python

def rightmove_webscrape(rightmove_url, rent_or_buy, dest_folder):
    # imports
    from lxml import html
    import requests
    import pandas as pd
    import datetime as dt

    # Initialise a pandas DataFrame to store the results
    df = pd.DataFrame(columns=['price', 'type', 'address', 'url'])

    # Get the total number of results returned by the search
    page = requests.get(rightmove_url)
    tree = html.fromstring(page.content)
    xp_result_count = '//span[@class="searchHeader-resultCount"]/text()'
    result_count = int(tree.xpath(xp_result_count)[0].replace(",", ""))

    # Convert the total number of search results into the number of iterations required for the loop
    loop_count = int(result_count / 24)
    if result_count % 24 > 0:
        loop_count = loop_count + 1

    # Set the Xpath variables for the loop
    if rent_or_buy == 'rent':
        xp_prices = '//span[@class="propertyCard-priceValue"]/text()'
    elif rent_or_buy == 'buy':
        xp_prices = '//div[@class="propertyCard-priceValue"]/text()'

    xp_titles = '//div[@class="propertyCard-details"]//a[@class="propertyCard-link"]//h2[@class="propertyCard-title"]/text()'
    xp_addresses = '//span[@data-bind="text: displayAddress"]/text()'
    xp_weblinks = '//div[@class="propertyCard-details"]//a[@class="propertyCard-link"]/@href'
    xp_date = '//span[@class="propertyCard-branchSummary-addedOrReduced"]/text()'

    # Start the loop through the search result web pages
    try:
        # Get the start & end of the web url around the index value
        start, end = rightmove_url.split('&index=')
        url_start = start + '&index='
        url_end = end[1:]
        for pages in range(0, loop_count, 1):
            url = url_start + str(pages * 24) + url_end
            page = requests.get(url)
            tree = html.fromstring(page.content)
    except ValueError:
        page = requests.get(rightmove_url)
        tree = html.fromstring(page.content)

    # Reset variables
    price_pcm, titles, addresses, weblinks, date = [], [], [], [], []

    # Create data lists from Xpaths
    for val in tree.xpath(xp_prices):
        price_pcm.append(val)
    for val in tree.xpath(xp_titles):
        titles.append(val)
    for val in tree.xpath(xp_addresses):
        addresses.append(val)
    for val in tree.xpath(xp_weblinks):
        weblinks.append('http://www.rightmove.co.uk' + val)
    for val in tree.xpath(xp_date):
        date.append(val)

    # Convert data to temporary DataFrame
    data = [price_pcm, titles, addresses, weblinks, date]
    temp_df = pd.DataFrame(data)
    temp_df = temp_df.transpose()
    temp_df.columns = ['price', 'type', 'address', 'url', 'ad_date']

    # Drop empty rows from DataFrame which come from placeholders in rightmove html
    if len(temp_df) > 0:  # This condition is required because rightmove tells you it has more results than it returns, and the below will error if temp_df is empty
        temp_df = temp_df[temp_df.url != 'http://www.rightmove.co.uk' + '/property-for-sale/property-0.html']
    # Join temporary DataFrame to main results DataFrame
        frames = [df, temp_df]
        df = pd.concat(frames)

    print(df.head()) #DEBUG
    # Renumber results DataFrame index to remove duplicate index values
    df = df.reset_index(drop=True)

    # Convert price column to numeric values for analysis
    df.price.replace(regex=True, inplace=True, to_replace=r'\D', value=r'')
    print(df.head())  # DEBUG
    df.price = pd.to_numeric(df.price)
    print(df.head())  # DEBUG
    # Extract postcode stems to a separate column
    df['postcode'] = df['address'].str.extract(r'\b([A-Za-z][A-Za-z]?[0-9][0-9]?[A-Za-z]?)\b', expand=True)

    # Extract number of bedrooms from 'type' to a separate column
    df['number_bedrooms'] = df.type.str.extract(r'\b([\d][\d]?)\b', expand=True)
    df.loc[df['type'].str.contains('studio', case=False, na=False), 'number_bedrooms'] = 0

    # Add in search_date column to record the date the search was run (i.e. today's date)
    now = dt.datetime.today().strftime("%d/%m/%Y")
    df['search_date'] = now

    # Export the results to CSV
    csv_filename = dest_folder + 'rightmove_' + rent_or_buy + '_results_' + str(
        dt.datetime.today().strftime("%Y_%m_%d %H %M %S")) + '.csv'
    df.to_csv(csv_filename, encoding='utf-8')

    # Print message to validate search has run showing number of results received and name of csv file.
    print(len(df), 'results saved as \'', csv_filename, '\'')

    return df



rightmove_url = 'http://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=STATION%5E1754&radius=0.5&propertyTypes=bungalow%2Cdetached%2Cflat%2Csemi-detached%2Cterraced&includeLetAgreed=false&areaSizeUnit=sqm'
rightmove_url = 'http://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E87490&numberOfPropertiesPerPage=24&radius=0.0&sortType=6&index=0&propertyTypes=detached%2Csemi-detached%2Cterraced%2Cflat%2Cbungalow&maxDaysSinceAdded=7&includeSSTC=false&viewType=LIST&areaSizeUnit=sqft&currencyCode=GBP'

data_folder = '/Users/duccioa/CLOUD/01_Cloud/01_Work/04_Projects/0031_CrossMap/05_Data/'

df = rightmove_webscrape(rightmove_url,'buy', data_folder)
m = df['price'].median()
s = df.groupby(['number_bedrooms'])['price'].median()

ss = []
for i in range(0,len(s)):
    ss.append(s[i]/int(s.index[i]))