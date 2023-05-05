# DataTalksClub - Data Engineering Project

## Bandcamp Sales
This project is based on the 1 million Bandcamp sales dataset.

### Goal
Goal of the project is to find the highest sales and highest average sales in each country.

### Answers
This project will answer the following questions:
* Which country spends the most on average where POS activity greater than 20?
    - Will use a table based off of sales_partitioned
* Which artist has the most sales in each country?
    - Will use a table based off of sales_clustered
* Which artist receives the highest USD in average POS by country.
    - Will use a table based off of sales_clustered


### Technology
* Infrastructure as Code
    - Terraform
* Workflow Orchestration
    - Prefect
* Data Lake
    - Google Cloud Storage
* Data Transformation
    - PySpark
    - DBT
* Data Warehouse
    - PySpark
    - Google BigQuery
* Charts
    - Looker Studio


## The following describes the Dataset
        * olumns:
          - name: intId
	    * A float number cut from _id.
          - name: _id
	    * A long reference the Artist name and extra identification.
	      - This column was split and the number format at the beginning was copied and used as the 'intId'.
          - name: datetime
          - name: country
          - name: ST
          - name: IT
          - name: item_price
          - name: amount_paid_usd
            * Amount paid by the customer
          - name: currency
          - name: AOF
          - name: art_id
          - name: releases
          - name: artist_name
            * Artist name we use for our data ingestion.
          - name: album_title
          - name: cc_ref
            * A iterative number given to the list of countries.
              - We first take Distinct countries from the Dataset and then create a suparate table - country_code_ref
                as a lookup table.
                This `cc_ref` is used as `RANGE_BUCKET` so that we can partition based off of country.

      - name: country_code_ref
        description: >
          A Reference to the RANGE_BUCKET id (cc_ref) as a lookup for country_code.
        columns:
          - name: count
          - name: contry_code
          - name: cc_ref

### Spark as a Data Transformation
PySpark is used for transforming data.
1) We will take the Bandcamp Dataset.
2) Find all distinct countries.
3) Add an iterative number to each country (cc_ref)
4) Drop some unnecessary columns from original dataset.
5) Send back to Google Cloud Storage.

#### PySpark used above may have been unnecessary here for this small dataset but used for learning purposes.

## Reproduce The Project
1) Create a Google Cloud Instance
2) Create identity key:
```
$ ssh-keygen
```
    - When answering hte questions for location, answer with `~/.ssh/dtc`
3) Copy the IP Address from your google cloud instance to your `~/.ssh/connfig` like so:
```bash
Host dtc
  Hostname X.X.X.X
  User app
  IdentityFile /home/<USERNAME>/.ssh/dtc
  SetEnv TERM=xterm-256color
```
    - Replace X.X.X.X with the IP Address given for your Remote Google Cloud Instance
X) SSH into your Google Cloud Instance:
```
$ ssh dtc
```
Then in the remote Google Cloud Instance execute the following commands:
```bash
$ git clone https://github.com/jrhamilton/bcsales`
$ bash bcsales/vm/SourceMe.sh
```
Follow the instructions. This will walk you through the building of your system.


### Start Tmux
```
$ cd ~/bcsales
$ tmux a -t bc
```
This will start a Tmux session
type `CTRL-B c`
  - This will put you in another window.
Now type `CTRL-B 0`
  - This will take you back to previous window.
Now type `CTRL-B 1` to go back the newer window.
`$ prefect orion start`
Now type `CTRL-B %`
  - This will create a new pane in same window.
  - This will be for your Prefect Agent.
`$ prefect agent start -q default`
Now type `CTRL-B c`
  - This will execute Tmux to put you in a new Window.


## Build Program
Execute the folowing commands
```
$ cd ~/bcsales/terraform
$ terraform init
$ terraform plan
$ terraform apply
```
  - Reply 'yes'
```
$ cd ~/bcsales
$ bash run_program.sh
```
When program is done building, execute the folowing commands:
```
$ cd ~/bcsales/dbt/bcsales
$ dbt build
$ dbt build -t prod
```
