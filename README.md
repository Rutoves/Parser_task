# Parser_task
Solution for the next task:

This test task represents a simple example of the data we work with and the type of problems you will be facing during real-life data development. The public database in question was created after the financial crisis of 2008 to store and make public consumer complaints, submitted to US Consumer Financial Protection Bureau (CFPB) by individuals unhappy about the products they have been sold and services provided by US retail banks.

The data is freely available, among other channels, at https://www.consumerfinance.gov/data-research/consumer-complaints. Please study various ways of loading that data and regular updates programmatically, and choose one. You should also choose how you would store data on your end, for which we suggest a relational database (preferably PostgreSQL).

Starting from scratch, your program should load the full history of complaints since inception. When the data is loaded (or when it has already been created in a previous program run), you will need to load only last month worth of complaints to update / amend the database (here, we assume that we will run the program regularly).

It is expected that in addition to new records which have been added since last run, you might encounter changes in the records you already have on your end. We propose the following versioning system: have a separate update_stamp column in addition to the columns provided by CFPB, where you will store the actual time you got and saved the record. For new records, just add them to your database. For updated records, add another row with new (obviously newer) update_stamp. For deleted rows, we suggest you create a new row filling all columns (apart from complaint_id and update_stamp) with NaNs, to indicate that the data is no longer there. Note, that you will have to work with the resulting table, which might have duplicated complaint_id's with different update_stamps, to construct the latest version of the data you have on your end, for the purpose of comparing records with CFPB.

You may assume that the number of amends will be low compared to the overall number of records. You will also notice that since we load only last month worth of complaints in the incremental regime, we are blind to changes in older records, but assume that this is an acceptable compromise.
Finally we need to be able estimate some data properties. Draw a graph counting updates each day (separate amends and new additions), and another counting number of complaints for two different companies over time.
