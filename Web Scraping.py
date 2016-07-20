import mechanize
from bs4 import BeautifulSoup
import re
from urllib2 import urlopen
import numpy as np
import math
import pandas as pd
from pandas import DataFrame
import MySQLdb
import pandas.io.sql as sql
from pygeocoder import Geocoder
######################zipcode##################################################
conn = MySQLdb.connect(host="localhost", # your host, usually localhost, or remote host
                     user="username", # your username
                      passwd="password", # your password
                      db="yp") # name of the data base
cases=sql.read_sql('select *  from Test_Cases ', conn) # assume you have a table to store search information.
for c in cases.index:
    # Get coordinate of address                                          
    address = cases.loc[c, :]["address"]
    results = Geocoder.geocode(address)
    Lat=results[0].coordinates[0]
    Lng=results[0].coordinates[1]                                          
    Radius = 10                                       
    sqrDis = 0.0002080188329 * Radius * Radius
    Lat = str(Lat)
    Lng = str(Lng)
    sqrDis = str(sqrDis)
    query="SELECT LU_Zip.ID, City, State, Zip, Latitude, Longitude, \
    SQRT((Latitude-"+ Lat + ")*(Latitude-" + Lat + ")+(Longitude-" + Lng +")*(Longitude-" + Lng + "))/0.0144722858 AS Distance \
    FROM `LU_Zip` INNER JOIN `LU_State` ON `LU_Zip`.StateID=`LU_State`.ID \
    WHERE ((Latitude-" + Lat + ")*(Latitude-" + Lat + ")+(Longitude-" + Lng + ")*(Longitude-" + Lng + "))<" + sqrDis + "ORDER BY Distance;"
    
    zip_table = sql.read_sql(query, conn)
    zips = zip_table["Zip"].tolist()
    
    #############################Web Scraping#######################################
    name_list = ["id", "business_name", "Street", "City", "State", "PostCode", "ServeRegion", "Telephone", "yp_Website", "own_Website", "Rating","Email", "Description", "Service"]
    table=DataFrame(columns= name_list)
    # for instance zips=[20904, ...]
    #search =raw_input("Enter the service to search:")
    search=cases.loc[c, :]["search"] # for instance, search=['roof  repair', ...]
    search_link=search.lower().replace(" ", "+")
    for zipcode in zips:
        
        #location =raw_input("Enter the location to search:")
        zipcode = str(zipcode)
        
        #location_link=location.lower().replace(" ", "+") 
        base_page="http://www.yellowpages.com/search?search_terms="+search_link+"&geo_location_terms="+zipcode+"&page="
        first_page=base_page+"1"+"&s=average_rating"
        first_html = urlopen(first_page).read()
        first_soup = BeautifulSoup(first_html, "html.parser")
        # Get page_list
        #tag = first_soup.find_all("p")
        sp=first_soup.find("span", text = "Showing")
        #print sp.next_sibling
        total=int(str(sp.next_sibling).split()[-1])
        #print total
        if total>=30:
            total =30
        pages = int(math.ceil(total/30.0))
        
        #print pages
        page_list =np.arange(pages) +1
        
        #############################################################################
        
        for i in page_list:
            url= base_page+str(i)+"&s=average_rating"
            html=urlopen(url).read()
            soup=BeautifulSoup(html,  'html.parser')
                
            for div in soup.find_all("div",class_="info"):
                
                id_list = [tag.string for tag in div.find("h3", class_="n") ]
                #business-name
                business_list=[tag.string for tag in div.find("a", class_="business-name")]
                #street
                try:
                    street_list =[tag.string for tag in div.find("p", {"itemprop":"address"}).find("span", {"itemprop":"streetAddress"})]
                except:
                    street_list=["None"]
                #city
                try:
                    city_list = [tag.string for tag in div.find("p", {"itemprop":"address"}).find("span", {"itemprop":"addressLocality"})]
                except:
                    city_list = ["None"]
                #State
                try:
                    state_list = [tag.string for tag in div.find("p", {"itemprop":"address"}).find("span", {"itemprop":"addressRegion"})]
                except:
                    state_list = ["None"]
                #zipcode
                try:
                    postcode_list = [tag.string for tag in div.find("p", {"itemprop":"address"}).find("span", {"itemprop":"postalCode"})]
                except:
                    postcode_list = ["None"]
                #serve region
                try:
                    serveregion_list = [tag.string for tag in div.find("p", class_="adr")]
                except:
                    serveregion_list = ["None"]
                #telephone
                try:
                    telephone_list = [tag.string for tag in div.find("div", attrs={"itemprop":"telephone"})]
                except:
                    telephone_list = ["None"]
                #website
                try:
                    website_list = [tag['href'] for tag in div.find_all("a", href=True, class_="business-name")]
                except:
                    website_list = ["None"]
                # More information and Email
                try:
                    MoreInformation_list = [tag['href'] for tag in div.find_all("a", href=True, text="More Info")]
                except:
                    MoreInformation_list = ["None"]
                # Personal website
                try:
                    Web_list = [tag['href'] for tag in div.find_all("a", href=True, text="Website")]
                    if Web_list==[]:
                        Web_list=["None"]
                except:
                    Web_list = ["None"]
                # Ratings
                try:
                    rating_list = [tag["class"] for tag in div.find("a",class_='rating')]
                except:
                    rating_list=["None"]

                
                
                if id_list != [None] and id_list !=[] and id_list[0].endswith('. '):
                    name = business_list[0]
                    number = id_list[0]
                    street = street_list[0]
                    city= city_list[0]
                    state=state_list[0]
                    postcode = postcode_list[0]
                    serveregion=serveregion_list[0] # This one needs some reasearch to best show
                    telephone=re.compile(r'[^\d.]+').sub('', telephone_list[0])
                    #telephone=telephone_list[0]
                    website=website_list[0]
                    own_website=Web_list[0]
                    rating=rating_list[0][1]+rating_list[0][2]+" stars"
                    try:
                        link = website[0]
                    except:
                        link = "N"
                    if link =="/":
                        yp_website="http://www.yellowpages.com" + website
                        
                    try:
                        MoreInformation = MoreInformation_list[0]
                    except:
                        MoreInformation = "None"
                    if MoreInformation[0] == "/":    
                        PersonalPage= "http://www.yellowpages.com"+website
                        br = mechanize.Browser()
                        Phtml = br.open(PersonalPage).read()
                    
                        Psoup = BeautifulSoup(Phtml)    
                    
                        try:
                            email = Psoup.find("a", text= "Email Business")["href"][7:]
                        except:
                            email = "None"
                        try:
                            description= Psoup.find("dd", class_="description").string
                            description=description.encode('utf-8')
                        except:
                            description= "None"
                        try:
                            services = ""
                            for node in Psoup.find("dd", class_="categories"):
                                services+=node.find("a", text=True).string+"; "
                                services=services.encode('utf-8')
                        except:
                            services= "None"
                    #print number + name+", "+ street +", "+ city+state+", " + postcode+", "+serveregion+", " + telephone,  website, email
                    #["id", "business_name", "Street", "City", "State", "PostCode", "ServeRegion", "Telephone", "Website", "Email"]
                    row = DataFrame(columns = name_list, index = np.arange(1))
                    row.loc[0, "id"]=number
                    row.loc[0, "business_name"]=name
                    row.loc[0, "Street"]= street
                    row.loc[0, "City"]= city
                    row.loc[0, "State"]= state
                    row.loc[0, "PostCode"]= postcode
                    row.loc[0, "ServeRegion"]=serveregion
                    row.loc[0, "Telephone"]=telephone
                    row.loc[0, "yp_Website"]=yp_website
                    row.loc[0, "own_Website"]=own_website
                    row.loc[0, "Rating"]=rating
                    row.loc[0, "Email"]= email
                    row.loc[0, "Description"]=description
                    row.loc[0, "Service"] =services
                    
                    table=pd.concat([table, row],axis=0)
                    table.reset_index(drop=True, inplace=True)
table.replace("None", "", inplace=True)   # replace "None" with blank
##########################Remote MySQL#######################################
conn = MySQLdb.connect(host="localhost", # your host, usually localhost
                     user="username", # your username
                      passwd="password", # your password
                      db="yp") # name of the data base

table.to_sql(con=conn, name='YP_Web_Scraping', if_exists='replace', flavor='mysql', index=False) #without index
#test_sample= sql.read_sql('select *  from Test_Web_Scraping ', conn)
#test_sample






        