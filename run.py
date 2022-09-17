import sqlite3
import pandas as pd


def modify_data(country_population_df, fully_vacc_by_ico_code_max_df):
    merged_tables = pd.merge(country_population_df, fully_vacc_by_ico_code_max_df, how='left', on=["iso_code", 'name']) \
        .fillna(0) \
        .rename(columns={'people_fully_vaccinated': 'total_vaccinated'})

    # we are able to do it without lambda and without if/else statement
    merged_tables['percentage_vaccinated'] = merged_tables.apply(
        lambda x: ((x['total_vaccinated'] / x['population']) * 100) if x['total_vaccinated'] != 0 else 0, axis=1) \
        .round(2)

    return merged_tables


def read_data():
    with open('data/country_populations.csv', newline='') as csv_country_pop_file:
        df = pd.read_csv(csv_country_pop_file, delimiter=',')
        country_population_needed_columns = ["Country Name", "Country Code", "2020"]

        country_population_df = df.loc[~df['Country Code'].str.startswith('OWID_'), country_population_needed_columns] \
            .rename(columns={'Country Code': 'iso_code', 'Country Name': 'name', '2020': 'population'})

    with open('data/vaccinations.csv', newline='') as csv_vacc_file:
        df = pd.read_csv(csv_vacc_file, delimiter=',')
        vacc_needed_columns = ["location", "iso_code", "people_fully_vaccinated"]

        fully_vacc_by_ico_code_max_df = df.loc[~df['iso_code'].str.startswith('OWID_'), vacc_needed_columns] \
            .rename(columns={'location': 'name'}) \
            .groupby(['name', 'iso_code'], as_index=False) \
            .agg(people_fully_vaccinated=('people_fully_vaccinated', 'max')).fillna(0)

    modified_data = modify_data(country_population_df, fully_vacc_by_ico_code_max_df)

    return modified_data


def write_data(merged_tables):
    with sqlite3.connect('zadacha.db') as cnx:
        merged_tables.to_sql('temp_table', con=cnx, if_exists='replace', index=False)

        # bonus task

        sql_update = """
             UPDATE countries
                 SET 
                 population = temp_table.population, 
                 total_vaccinated = temp_table.total_vaccinated, 
                 percentage_vaccinated = temp_table.percentage_vaccinated
                 FROM temp_table
                 WHERE countries.iso_code = temp_table.iso_code
         """

        sql_insert = """
        INSERT INTO countries SELECT * FROM  temp_table WHERE NOT EXISTS (SELECT *
                  FROM countries
                  WHERE temp_table.iso_code = countries.iso_code)
         """

        sql_drop_temp = """
        DROP TABLE temp_table;
        """

        with cnx as connection:
            connection.execute(sql_update)

        with cnx as connection:
            connection.execute(sql_insert)

        with cnx as connection:
            connection.execute(sql_drop_temp)


def run():
    merged_tables = read_data()
    write_data(merged_tables)
    print('Successfully added!')


if __name__ == '__main__':
    run()
