# ipums2db
Convert IPUMS data extracts to database dump files

## description
`ipums2db` converts IPUMS fixed-width file extracts to relational database dump files. 

Per IPUMS, the [Integrated Public Use Microdata Series](https://www.ipums.org/)...
> "provides census and survey data from around the world integrated across time and space. IPUMS integration and documentation makes it easy to study change, conduct comparative research, merge information across data types, and analyze individuals within family and community contexts. Data and services available free of charge."

These files can often get very large, and loading the entire dataset into memory can either be cumbersome or not possible in some cases. `ipums2db` allows you to work with your ipums data in a traditional database system environment with the added (if you like SQL of course) bonus of using SQL syntax to query your data. 

## installation

### homebrew (recommended)
```bash
$ brew install rhawrami/ipums2db/ipums2db
```

### manual installation
Prebuilt distributions for most platforms/architecutes are available in the [releases page](https://github.com/rhawrami/ipums2db/releases).

### go install
```bash
$ go install github.com/rhawrami/ipums2db/cmd@latest

# this installs ipums2db under the name `cmd`
$ which cmd
$ ~/go/bin/cmd

# let's change the name
$ mv ~/go/bin/cmd ~/go/bin/ipums2db
```
## usage

```bash
$ ipums2db --help
Usage: ipums2db [options...] -x <xml> <dat>
Flags:
 -x <xml>                     DDI XML path (mandatory)
 -b <dbType>                  Database type (default 'postgres')
 -t <tabName>                 Table name (default 'ipums_tab')
 -i <idx1[,idx2]>             Variable[s] to index on (default no idx)
 -d                           Make directory format (default false)
 -o <outFileOrDir>            File/Directory to output (default 'ipums_dump.sql')
 -s                           Silent output (default false)

Full Usage Example:
 ipums2db -b mysql -t mytab -i age,sex -o mydump.sql -x myACS.xml myACS.dat
For more information, visit https://github.com/rhawrami/ipums2db
```
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
#### `-b <databaseName>`
- Name of your database system; currently supported options include:

    1. `postgres`
    2. `mysql`
    3. `mssql`
    4. `oracle`
- Defaults to `postgres`

#### `-t <tableName>`
- Name that the database table should be
- Defaults to `ipums_tab`

#### `-i <[singleIndexCol | indexCol1,indexCol2]>`
- Indices to create; as of now, only single-column indices are supported; additionally, only the default database index structure (usually b+ tree) is supported; to create multiple single-column indices, **separate variable names by a comma**; to create just one index, simply input the column name for that variable
- Defaults to `""`

#### `-d`
- Boolean flag: instead of single ".sql" dump file, create dump directory with "schema" and inserts.
- For very large files, a single sql dump file can be a bit cumbersome to load (note: not impossible, just annoying to wait on a single file to load). To both speed up the program (e.g., allow multiple dump file writers, one for each dump file) and the eventual database inserts, a directory is created, with a single `ddl.sql` file (includes main table creation, index creation, and ref_table creation and inserts), and a variable number of insertion files. Each insertion file holds at most around 10 GiB, so processing a 24 GiB fixed-width file with `-d` would produce 3 insertion files, each of the form `inserts_{i}.sql`.

#### `-o <[outputFile | directory name]>`
- In case of one output file: name that the dump file should be
- In case of directory format: name of the output directory
- Defaults to `ipums_dump.sql | ipums_dump/`

#### `-s`
- silent boolean flag; will silence standard output messages
- defaults to `false`

### example usage
1. no optional arguments provided:
```bash
$ ipums2db -x data/cps/asec_rand00.xml data/cps/asec_rand00.dat
=================================
dbT: postgres
tab: ipums_tab
idx:
xml: data/cps/asec_rand00.xml
dat: data/cps/asec_rand00.dat
=================================
Time elapsed: 8.414s (683.23 MiB/s)

# check file
$ du -h ipums_dump.sql
7.5G	ipums_dump.sql
```

2. databaseType: `mysql`; tabName `棕熊`; indices: `age,sex,year`
```bash
$ ipums2db -b mysql -t 棕熊 -i age,sex,year -x data/usa/acs_031323.xml data/usa/acs_031323.dat
================================
dbT: mysql
tab: 棕熊
idx: age,sex,year
xml: data/usa/acs_031323.xml
dat: data/usa/acs_031323.dat
================================
Time elapsed: 1.526s (618.63 MiB/s)
```

3. databaseType: `mssql`; outFile: `gimmeMyFileQuick.sql`; silent: `true`
```bash
$ ipums2db -b mssql -o gimmeMyFileQuick.sql -s -x data/health/nhis_rand.xml data/health/nhis_rand.dat

$ du -h gimmeMyFileQuick.sql
2.0G	gimmeMyFileQuick.sql
```

4. databaseType: `oracle`; makeItDir: `true`; outFile: `prettyBigDir`
```bash
$ ipums2db -b oracle -d -o prettyBigDir  -x data/cps/cps_monthlybig.xml data/cps/cps_monthlybig.dat
====================================
dbT: oracle
tab: ipums_tab
idx:
xml: data/cps/cps_monthlybig.xml
dat: data/cps/cps_monthlybig.dat
====================================
Time elapsed: 19.655s (555.84 MiB/s)

# look at dir format
$ du -h prettyBigDir/*
 72K	prettyBigDir/ddl.sql
7.4G	prettyBigDir/inserts_0.sql
7.4G	prettyBigDir/inserts_1.sql
```

### to-do
1. Allow for schema creation from either CSV or DDI files.
2. Allow for multi-column index creation.
3. Allow for filtering while parsing through the fixed-width file; something like `-f sex=1`

### limitations
- As noted above, fixed-width files must be decompressed prior to running the program.
- Currently, there is no check on if you pass the correct pair of DDI and fixed-width files. You can pass an irrelevant IPUMS DDI to a fixed width file it's *supposed to match*, and it'll generate a result, but it certainly won't load into any database.
- Depending on the character/byte width of a variable, some variables which would intuitively be (or are listed as) integer types will instead be character types. This should be rare, but it can happen.
- Error handling on file writes is not that sophisticated at the moment. Given that errors at this step should be rare, this is not that big of an issue, but it's
still not ideal.