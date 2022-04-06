from connection import DatabaseQuery
from csv import DictWriter
from datetime import datetime


def get_code():
    school_code = input('Select schoolcode> ').upper()

    if school_code in ['AMH', 'UMA', "SMT", 'MHC', 'HAM', 'DEP']:
        return school_code
    else:
        print('invalid school abbreviation ')


def parse_date(date):
    return datetime.strptime(str(date), '%Y%m%d')

def barcode_parse(barcode, school_code):
    # remove whitespace in barcodes. If the barcode doesn't match standard barcode length, append school code
    barcode = barcode.replace(" ", "")
    if len(barcode) < 14:
        barcode = f"{barcode}-{school_code}"
    # print(barcode)
    return barcode


def querystring(school_code):
    print("querying...")
    querystring = f'''SELECT RTRIM(substr(barcode.Z308_REC_KEY,3)) as patron_barcode, 
    RTRIM(item.Z30_BARCODE) as item_barcode, 
    loan.Z36_LOAN_DATE, 
    loan.Z36_DUE_DATE, 
    CASE loan.Z36_STATUS
        WHEN 'C' then 'CLAIMED RETURNED'
        WHEN 'L' then 'LOST'
        WHEN 'A' then  'Checked out' 
        ELSE 'SOMETHING IS WRONG' END as loan_status
    FROM {school_code}50.Z36 loan
    inner join FCL00.Z308 barcode
    on loan.Z36_ID = barcode.Z308_id and substr(barcode.Z308_REC_KEY,0,2) = '01'
    inner join {school_code}50.Z30 item
    on item.Z30_REC_KEY =  loan.Z36_REC_KEY
    order by loan.Z36_STATUS, PATRON_BARCODE, loan.Z36_LOAN_DATE'''
    return querystring



def main():
   
    school_code = get_code()
    open_loans = DatabaseQuery(querystring(school_code))

    results = open_loans.search()
    headers = open_loans.headers
    now = datetime.now().strftime('%Y-%m-%d--%H')
    with open(f'{school_code}-{now}-loans.csv', 'w', newline='') as output:
        outputcsv = DictWriter(output, fieldnames=headers)
        print(headers)
        outputcsv.writeheader()
        count = 0
        report = {
            'Checked out': 0,
            'LOST': 0,
            'CLAIMED RETURNED': 0
        }
        for line in results:
            report[line['LOAN_STATUS']] += 1
            line.update({'Z36_LOAN_DATE': parse_date(
                line['Z36_LOAN_DATE']), 'Z36_DUE_DATE': parse_date(line['Z36_DUE_DATE']),
                'ITEM_BARCODE': barcode_parse(line['ITEM_BARCODE'], school_code) 
                })
            outputcsv.writerow(line)
            count += 1
            if count % 100 == 0:
                print(count)
    print(report, count)


if __name__ == '__main__':
    main()
