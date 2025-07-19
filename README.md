# Endocarditis
This file is used to read all the SAS codes and tables I've written for the infective endocarditis project
You can also find these SAS codes and tables in the ANH_EI and ENDOCARDITE/ANH folders in SNDS/SAS portail (However they are not cleaned yet so it is preferable to take the codes from my github)

#All the final tables I've already stocked in ENDOCARDITE/ANH/cohort_july. Please ask me before using any tables besides the tables in this folder. 

#Create exposed groups
You will find 4 code files to (creer_groupe_..) to create 4 exposed risks for this project. These groups will be used for the file avant_appariement

#Import data from SAS to R for the matching process
As the tables (including exposed and non-exposed subjects) are very large, you need to cut the table into multiple small tables and import them to R. I did adapt the R codes in the matching package of Stephane Le Vu to accounting for the fact that the non-expose subjects need to be alive at least until the matching date.

#Create clean tables from the matched sets exported from R
