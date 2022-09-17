# Task
- From the country populations data (data/country_populations.csv) export each country population (everyone, that DON'T start with an "OWID_" prefix) for 2020. 

- From the vaccinations data file (data/vaccinations.csv) export the fully vaccinated population ("people_fully_vaccinated" column) for each of the countries defined in "country_populations.csv" (without the regions defined with the "OWID_" prefix). Use the coulmns - iso_code / Country Code. If there are countries that are not included in the vaccinations file fill their data with zeros (0).

- Calculate the percentage of fully vaccinated people for each country: ([vaccinations.csv\column "people_fully_vaccinated"] / [country_populations.csv\column "2020"])*100

- Store/write the information in the given table, following the predefined structure of the table:

Table 'countries':
name (text), iso_code (text), population (int), total_vaccinated (int), percentage_vaccinated(real) 

- 'prepare.py' creates a sqlite3 table with couple of records.

- Bonus: Try to leave the predefined records intact, while just updating their values for population, total_vaccinated and percentage_vaccinated.
