import csv
import random
import argparse
import datetime as dt
from faker import Faker


def write_row(writer, *args):
    writer.writerow(args)


# Initialize Faker
fake = Faker()

# Define the number of rows you want to generate
num_rows = 50
# num_rows = 100

# Define the CSV file name
file_output = "lib/tools/data.csv"

# Define the headers
headers = [
    "rrn",
    "stateId",
    "assetId",
    "id",
    "name",
    "accountId",
    "accountName",
    "cloudType",
    "regionId",
    "regionName",
    "service",
    "resourceType",
    "startDate",
    "endDate",
    "sheet_name",
]

parser = argparse.ArgumentParser()
parser.add_argument("-r", "--rows", help="number of rows to generate")
parser.add_argument(
    "-o", "--output", help="path and filename to output to (should end in .csv for now)"
)
# parser.add_argument("-h", "--headers", help="comma-separated list of header values")
# parser.add_argument("-s", "--start_date", dest="start_date", nargs='?', const=dt.datetime(2024,1,1,0,00), type=lambda s: dt.datetime.strptime(s, '%Y-%m-%d'), help="Date in the format yyyy-mm-dd")
# parser.add_argument("-e", "--end_date", dest="end_date", nargs='?', const=dt.datetime(2024,1,1,0,00), type=lambda s: dt.datetime.strptime(s, '%Y-%m-%d'), help="Date in the format yyyy-mm-dd")
# parser.add_argument("-d", "--daily", action='store_true' , help="Inserts into datasource information from yesterdays date")
# parser.add_argument("-i", "--initialize", action='store_true' , help="Initializes db table")
# parser.add_argument("-b", "--between", dest="between", nargs=2 )
# parser.add_argument("-t", "--test", action='store_true' , help="Tests connection to database")
## Mandatory
# parser.add_argument("files", type=str, help='yaml files or folders to be considered for querying data. can be absolute or relative paths')
args = parser.parse_args()

if args.rows != None:
    num_rows = int(args.rows)

if args.output != None:
    file_output = args.output

# Generate the data and write to the CSV file
with open(file_output, "w+", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(headers)

    for _ in range(num_rows):
        start_dt = fake.date_between(
            start_date=dt.datetime(2020, 1, 1, 00, 00, 00),
            end_date=dt.datetime(2024, 12, 1, 00, 00, 00),
        )
        end_dt = fake.date_between(start_date=start_dt)
        name = fake.domain_word()
        uuid = fake.uuid4()
        rrn = fake.name()
        state_id = fake.md5()
        asset_id = fake.md5()
        id = f"/subscriptions/{uuid}/resourceGroups/{name}"
        name = name
        account_id = uuid
        account_name = name
        cloud_type = "azure"
        region_id = "eastus"
        region_name = "Azure East US"
        service = "Azure Resource Manager"
        resource_type = "Other"
        start_date = start_dt
        end_date = end_dt
        sheet_name = fake.file_name(extension="csv")

        writer.writerow(
            [
                rrn,
                state_id,
                asset_id,
                id,
                name,
                account_id,
                account_name,
                cloud_type,
                region_id,
                region_name,
                service,
                resource_type,
                start_date,
                end_date,
                sheet_name,
            ]
        )

print(f"CSV file {file_output} generated successfully.")

