## Variables config to create view
cryptocurrency : str >> BTC, ADA, LINK, DOGE

frequency_partition : str >> Month, Quarter, Year

select_year : list >> [] is select all, [2018, 2019, 2020, 2021, 2022] select only year in the list

<b>Example</b>

cryptocurrency : "BTC"

frequency_partition : "Quarter"

select_year : [2021]

This will create views that contain each quarter of BTC data in year 2021.
