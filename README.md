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


### Technologies
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
X) Create identity key:
In your local machine do the following (OR SKIP THIS AND USE A KEY YOU ALREADY HAVE IF YOU PREFER)
```
$ ssh-keygen
```
    - When answering hte questions for location, answer with `~/.ssh/dtc`

X) In Google Cloud, go to 'META'
    - May need to search for meta (or ssh keys)
    - Add your ssh public key (~/.ssh/id.rsa.pub) to the metadata page
        - IMPORTANT: Make sure the key name you upload ends in '.pub'

X) Create a Google Cloud Instance
    - Search for instances and go to the VM Instance page.
    - Click Create An Instance
    - Select your region and keep note of the region name (ignoring the content inside the paranthesis)
    - Under machine Series, select: E2
    - Under machine Type, select: e2-standard-2 (OR e2-medium)
        - This gives 8 GB memory (or 4 GB)
    - Under Boot Disk:
        - Operating System: Ubuntu
        - Version: 22.04 LTS (x86/64)
        - Boot Disk Type: SSD Persistent Disk
        - Size: 30 GB
        - click 'SELECT'
    - Under 'Identity and API Access':
        - Select your service account email best used for GCS and BigQuery operation.
    - click 'CREATE'
    - Verify your instance was created.
    - Copy the external IP Address shown on the next page after the Instance is created.
        - You may have to click 'View Network Details' in the 3-dot menu to see the IP Address.

X) Copy the IP Address from your google cloud instance to your `~/.ssh/config` like so:
```bash
Host dtc
  Hostname X.X.X.X
  User <REMOTE-USERNAME>
  IdentityFile /home/<LAPTOP/COMPUTER USERNAME>/.ssh/dtc
  SetEnv TERM=xterm-256color
```
  - Ignore the above if you do not have Linux or Mac.
  - Replace X.X.X.X with the IP Address given for your Remote Google Cloud Instance

X) SSH into your Google Cloud Instance:
```
$ ssh dtc
```
  - OR:
```
$ ssh <REMOTE-USERNAME>@X.X.X.X
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
