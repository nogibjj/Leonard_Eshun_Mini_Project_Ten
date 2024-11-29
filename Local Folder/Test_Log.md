### Extraction Test ### 
Removing existing CSV file exists <br />Confirming that CSV file doesn't exists... <br />Test Successful <br />Extracting data and saving... <br />Testing if CSV file exists... <br />Extraction Test Successful


### Transform and Load Test ### 
Creating non-lookup table: air_quality <br />Creating lookup table: indicator <br />Creating lookup table: geo_data <br />Tables created. <br />Skipping the first row... <br />Inserting table data... <br />Inserting table data completed <br />Transform and Load Test Successful


### One Record Reading Test ### 
Executing query... <br />
```sql
select * from air_quality where air_quality_id = 740885
```

Asserting that row[0][data_value] == 16.4 <br />Assert Successful <br />One Record Reading Test Successful


### All Records Reading Test ### 
Executing query... <br />
```sql
select * from air_quality
```

Asserting that len(rows) == 18016 <br />All Records Reading Test Successful


### Record Saving Test ### 
Asserting there's no record in geo_data with ID 100000 <br />Executing query... <br />
```sql
select * from geo_data where geo_id = 100000
```

Assert Successful <br />Saving new record with ID 100000 <br />Executing query... <br />
```sql
SELECT name FROM pragma_table_info('geo_data')
```

Executing query... <br />
```sql
INSERT INTO geo_data (geo_id, geo_place_name, geo_type_name) VALUES ('100000', 'Lancaster', 'UFO')
```

Asserting there's now a record in geo_data with ID 100000 <br />Executing query... <br />
```sql
select * from geo_data where geo_id = 100000
```

Assert Successful <br />Record Saving Test Successful


### Record Update Test ### 
Asserting 'geo_place_name' in geo_data for row ID 100000 is 'Lancaster' <br />Executing query... <br />
```sql
select * from geo_data where geo_id = 100000
```

Assert Successful <br />Updating 'geo_place_name' in geo_data for row ID 100000 is 'Duke' <br />Executing query... <br />
```sql
UPDATE geo_data SET geo_place_name='Duke' WHERE geo_id = 100000
```

Asserting 'geo_place_name' in geo_data for row ID 100000 is now 'Duke' <br />Executing query... <br />
```sql
select * from geo_data where geo_id = 100000
```

Assert Successful <br />Record Update Test Successful


### Record Deletion Test ### 
Asserting there's a record in geo_data for row ID 100000 <br />Executing query... <br />
```sql
select * from geo_data where geo_id = 100000
```

Assert Successful <br />Deleting record with ID 100000 <br />Executing query... <br />
```sql
delete from geo_data where geo_id = 100000
```

Asserting there's no record in geo_data with ID 100000 <br />Executing query... <br />
```sql
select * from geo_data where geo_id = 100000
```

Assert Successful <br />Record Deletion Test Successful


### Reading All Column Test ### 
Executing query... <br />
```sql
SELECT name FROM pragma_table_info('air_quality')
```

Asserting the air_quality table has six (6) columns <br />Assert Successful <br />Reading All Column Test Successful


Executing query... <br />
```sql
select * from geo_data
```

Executing query... <br />
```sql
select * from indicator
```

