# ipums2db
Convert IPUMS data extracts to database dump files

## description
`ipums2db` is a program that converts IPUMS fixed-width file extracts to relational database dump files. 

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
#### `-db \<database name\>`
- Name of your database system; currently supported options include:

    1. `postgres`
    2. `mysql`
    3. `mssql`
    4. `oracle`
- Defaults to `postgres`

#### `-t \<table name\>`
- Name that the database table should be
- Defaults to `ipums_tab`

#### `-i \<indexCol1,indexCol2\>`
- Indices to create; as of now, only single-column indices are supported; additionally, only the default database index structure (usually b+ tree) is supported; to create multiple single-column indices, **separate variable names by a comma**; to create just one index, simply input the column name for that variable
- Defaults to no index creations

#### `-o \<output file name\>`
- Name that the dump file should be
- Defaults to `ipums_dump.sql`

#### `-s`
- silent boolean flag; will silence standard output messages
- defaults to false

### example usage
1. no optional arguments provided:
```bash
$ ipums2db -x data/cps_777.xml data/cps_777.dat
====================
dbT: postgres
tab: ipums_tab
idx:
xml: data/cps_777.xml
dat: data/cps_777.dat
====================
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

## coming soon
- **Directory format output**:

    - For very large files, a single sql dump file can be a bit cumbersome to load (note: not impossible, just annoying to wait on a single file to load). To both speed up the program (e.g., allow multiple dump file writers, one for each dump file) and the eventual database inserts, allow for a directory format structure for output:
    ```bash
    $ ls -1 ipums_dump/
    ddl.sql # includes table and index creations
    inserts_0.sql # first insert dump file
    inserts_1.sql
    inserts_2.sql
    ...
    inserts_N.sql # last insert dump file
    ```