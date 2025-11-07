# ipums2db
Convert IPUMS data extracts to database dump files

## description
`ipums2db` converts IPUMS fixed-width file extracts to relational database dump files. 

Per IPUMS, the [Integrated Public Use Microdata Series](https://www.ipums.org/)...
> "provides census and survey data from around the world integrated across time and space. IPUMS integration and documentation makes it easy to study change, conduct comparative research, merge information across data types, and analyze individuals within family and community contexts. Data and services available free of charge."

These files can often get very large, and loading the entire dataset into memory can either be cumbersome or not possible in some cases. `ipums2db` allows you to work with your ipums data in a traditional database system environment with the added (if you like SQL of course) bonus of using SQL syntax to query your data. 

## usage
To properly convert your extract, you must have two files:

1. A fixed width file holding your data (most often with a ".dat" extension); as of now, you will need to decompress your data prior to using it with `ipums2db`; this is fairly simple to do:
```bash
$ gunzip -k mydatfile.dat.gz
```
2. A data definition initiative (DDI) in XML format. This file should be readily downloadable with your fixed-width file extract from IPUMS.

The program syntax itself is fairly simple: provide the `-x` flag to your xml, and have the only argument be the path to your fixed width file. For example:
```bash
$ ipums2db -x data/cps_777.xml data/cps_777.dat
```
Here we see our input configuration, and output summary. There are a number of flags available:
### flags
#### `-db <database name>`
- Name of your database system; currently supported options include:

    1. `postgres`
    2. `mysql`
    3. `mssql`
    4. `oracle`
- Defaults to `postgres`

#### `-t <table name>`
- Name that the database table should be
- Defaults to `ipums_tab`

#### `-i <indexCol1,indexCol2>`
- Indices to create; as of now, only single-column indices are supported; additionally, only the default database index structure (usually b+ tree) is supported; to create multiple single-column indices, **separate variable names by a comma**; to create just one index, simply input the column name for that variable
- Defaults to `""`

### `-d`
- Boolean flag: instead of single ".sql" dump file, create dump directory with "schema" and inserts.
- For very large files, a single sql dump file can be a bit cumbersome to load (note: not impossible, just annoying to wait on a single file to load). To both speed up the program (e.g., allow multiple dump file writers, one for each dump file) and the eventual database inserts, a directory is created, with a single `ddl.sql` file (includes main table creation, index creation, and ref_table creation and inserts), and a variable number of insertion files. Each insertion file holds at most around 10 GiB, so processing a 24 GiB fixed-width file with `-d` would produce 3 insertion files, each of the form `inserts_{i}.sql`.
- NOTE: processing fixed-width files of size greater than 10 GiB will default to directory format, whether or not the flag is provided.

#### `-o <output file/directory name>`
- In case of one output file: name that the dump file should be
- In case of directory format: name of the output directory
- Defaults to `ipums_dump.sql | ipums_dump/`

#### `-s`
- silent boolean flag; will silence standard output messages
- defaults to `false`

### example usage
1. no optional arguments provided:
```bash
$ ipums2db -x data/cps_777.xml data/cps_777.dat
=====================
dbT: postgres
tab: ipums_tab
idx:
xml: data/cps_777.xml
dat: data/cps_777.dat
=====================
Time elapsed: 10.002s (410.07 MiB/s)
Dump written to: ipums_dump.sql
```

2. databaseType: `mysql`; tabName `棕熊`; indices: `age,sex,year`
```bash
$ ipums2db -x data/cps_777.xml -db mysql -t 棕熊 -i age,sex,year data/cps_777.dat
====================
dbT: mysql
tab: 棕熊
idx: age,sex,year
xml: data/cps_777.xml
dat: data/cps_777.dat
====================
Time elapsed: 10.049s (408.15 MiB/s)
Dump written to: ipums_dump.sql
```

3. databaseType: `oracle`; outFile: `gimmeMyFileQuick.sql`; silent: `true`
```bash
$ ipums2db -x data/cps_777.xml -db oracle -o gimmeMyFileQuick.sql -s data/cps_777.dat
$ ls *.sql
gimmeMyFileQuick.sql
```

3. databaseType: `oracle`; makeItDir: `true`; outFile: `myDumpDir`
```bash
$ ipums2db -x data/cps_777.xml -db oracle -d -o myDumpDir data/cps_777.dat
=========================
dbT: oracle
tab: ipums_tab
idx:
xml: data/cps_777.xml
dat: data/cps_777.dat
=========================
Time elapsed: 30.821s (407.38 MiB/s)
Dump written to: myDumpDir
$ ls -1 myDumpDir/
ddl.sql
inserts_0.sql
inserts_1.sql
```