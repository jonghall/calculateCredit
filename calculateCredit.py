import time,  SoftLayer, configparser, os, argparse, csv, math,logging, pytz
from datetime import datetime, timedelta


def convert_timedelta(duration):
    days, seconds = duration.days, duration.seconds
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    totalminutes = round((days * 1440) + (hours * 60) + minutes + (seconds/60),1)
    return totalminutes


def convert_timestamp(sldate):
    formatedDate = sldate
    formatedDate = formatedDate[0:22]+formatedDate[-2:]
    formatedDate = datetime.strptime(formatedDate, "%Y-%m-%dT%H:%M:%S%z")
    return formatedDate

def getDescription(categoryCode, detail):
    for item in detail:
        if item['categoryCode']==categoryCode:
            return item['description']
    return "Not Found"

def initializeSoftLayerAPI(user, key, configfile):
    if user == None and key == None:
        if configfile != None:
            filename=args.config
        else:
            filename="config.ini"
        config = configparser.ConfigParser()
        config.read(filename)
        client = SoftLayer.Client(username=config['api']['username'], api_key=config['api']['apikey'])
    else:
        client = SoftLayer.Client(username=user, api_key=key)
    return client


#
# Get APIKEY from config.ini & initialize SoftLayer API
#


## READ CommandLine Arguments and load configuration file
parser = argparse.ArgumentParser(description="z.")
parser.add_argument("-u", "--username", help="SoftLayer API Username")
parser.add_argument("-k", "--apikey", help="SoftLayer APIKEY")
parser.add_argument("-c", "--config", help="config.ini file to load")
parser.add_argument("-s", "--startdate", help="start date mm/dd/yy")
parser.add_argument("-e", "--enddate", help="End date mm/dd/yyyy")
parser.add_argument("-o", "--output", help="Outputfile")
parser.add_argument("-v", "--vsicredit", help="Credit Hours")
args = parser.parse_args()

client = initializeSoftLayerAPI(args.username, args.apikey, args.config)

## if no dates provided use previous month


if args.startdate == None:
    today = datetime.today()
    last_day_previous_month = today - timedelta(days=today.day)
    first_day_previous_month = last_day_previous_month - timedelta(days=last_day_previous_month.day - 1)
    startdate = datetime.strftime(first_day_previous_month, "%m/%d/%Y")
else:
    startdate=args.startdate

if args.enddate == None:
    today = datetime.today()
    last_day_previous_month = today - timedelta(days=today.day)
    enddate= datetime.strftime(last_day_previous_month, "%m/%d/%Y")
else:
    enddate=args.enddate

if args.output == None:
    outputname="calculateCredit.csv"
else:
    outputname=args.output

if args.vsicredit == None:
    vsicredit=2
else:
    vsicredit=float(args.vsicredit)

fieldnames = ['InvoiceID', 'BillingItemId', 'TransactionID', 'Datacenter', 'Product', 'Cores', 'Memory', 'Disk', 'OS', 'Hostname',
              'CreateDate', 'CreateTime', 'ProvisionedDate',
              'ProvisionedTime', 'ProvisionedDelta', 'CancellationDate', 'CancellationTime', 'HoursUsed', 'HourlyRecurringFee', 'CreditHours', 'ActualCreditHours', 'CreditAmount']

outfile = open(outputname, 'w')
csvwriter = csv.DictWriter(outfile, delimiter=',', fieldnames=fieldnames)
csvwriter.writerow(dict((fn, fn) for fn in fieldnames))
## OPEN CSV FILE TO READ LIST OF SERVERS

central = pytz.timezone("US/Central")

logfile="calculateCredits.log"
logging.basicConfig(filename=logfile, format='%(asctime)s %(message)s', level=logging.WARNING)
logging.warning ('Calculating Credits for Provisioning Events between %s and %s.' % (startdate, enddate))


InvoiceList = client['Account'].getInvoices(mask='createDate,typeCode, id, invoiceTotalAmount', filter={
        'invoices': {
            'createDate': {
                'operation': 'betweenDate',
                'options': [
                     {'name': 'startDate', 'value': [startdate+" 0:0:0"]},
                     {'name': 'endDate', 'value': [enddate+" 23:59:59"]}
                     ],
                },
            'typeCode': {
                'operation': 'in',
                'options': [
                    {'name': 'data', 'value': ['ONE-TIME-CHARGE', 'NEW']}
                ]
                },
            }
        })


for invoice in InvoiceList:
    invoiceID = invoice['id']
    invoicedetail=""
    logging.warning('Getting invoice detail for invoice %s.' % (invoiceID))
    while invoicedetail is "":
        try:
            time.sleep(1)
            invoicedetail = client['Billing_Invoice'].getObject(id=invoiceID, mask="closedDate, invoiceTopLevelItems, invoiceTopLevelItems.product,invoiceTopLevelItems.location")
        except SoftLayer.SoftLayerAPIError as e:
            logging.warning("Billing_Invoice::getObject: %s, %s" % (e.faultCode, e.faultString))
            time.sleep(10)

    invoiceTopLevelItems=invoicedetail['invoiceTopLevelItems']
    invoiceDate=convert_timestamp(invoicedetail["closedDate"])
    for item in invoiceTopLevelItems:
        if item['categoryCode']=="guest_core":
            itemId = item['id']
            billingItemId = item['billingItemId']
            location=item['location']['name']
            hostName = item['hostName']+"."+item['domainName']
            createDateStamp = convert_timestamp(item['createDate'])
            product=item['description']
            cores=""
            billing_detail=""
            logging.warning('Getting detail for billingitemId %s.' % (billingItemId))
            while billing_detail is "":
                try:
                    time.sleep(1)
                    billing_detail = client['Billing_Invoice_Item'].getFilteredAssociatedChildren(id=itemId,mask="product,categoryCode,description")
                except SoftLayer.SoftLayerAPIError as e:
                    logging.warning("Billing_Invoice_Item::getFilteredAssociatedChildren: %s, %s" % (e.faultCode, e.faultString))
                    time.sleep(5)

            os=getDescription("os", billing_detail)
            memory=getDescription("ram", billing_detail)
            disk=getDescription("guest_disk0", billing_detail)


            if 'product' in item:
                product=item['product']['description']
                cores=item['product']['totalPhysicalCoreCount']

            billingInvoiceItem=""
            logging.warning('Getting item detail for itemId %s.' % (itemId))
            while billingInvoiceItem is "":
                try:
                   time.sleep(1)
                   billingInvoiceItem = client['Billing_Invoice_Item'].getBillingItem(id=itemId, mask="cancellationDate, provisionTransaction")
                except SoftLayer.SoftLayerAPIError as e:
                   logging.warning("Billing_Invoice_Item::getBillingItem: %s, %s" % (e.faultCode, e.faultString))
                   time.sleep(5)

            if 'provisionTransaction' in billingInvoiceItem:
                provisionTransaction = billingInvoiceItem['provisionTransaction']
                provisionId = provisionTransaction['id']
                guestId = provisionTransaction['guestId']
                provisionDateStamp = convert_timestamp(provisionTransaction['modifyDate'])
            else:
                provisionTransaction = "0"
                provisionId = "0"
                guestId = "0"
                provisionDateStamp = convert_timestamp(item['createDate'])

            # determine cancelation date of VSI to calculate total hours; otherwise assume still running
            if 'cancellationDate' in billingInvoiceItem:
                if billingInvoiceItem['cancellationDate'] != "":
                    cancellationDateStamp=convert_timestamp(billingInvoiceItem['cancellationDate'])
            else:
                cancellationDateStamp="Running"

            # If still running use current timestamp to calculate hoursUsed otherwise use cancellation date.

            if cancellationDateStamp == "Running":
                    currentDateStamp=datetime.datetime.now()
                    hoursUsed=math.ceil(convert_timedelta(currentDateStamp-provisionDateStamp)/60)
            else:
                    hoursUsed=math.ceil(convert_timedelta(cancellationDateStamp-provisionDateStamp)/60)

            # Calculate Credit if hoursUsed greater than credit offer otherwise credit actual hours
            if hoursUsed >= vsicredit:
                   actualCreditHours = vsicredit
            else:
                   actualCreditHours = hoursUsed

            # CALCULATE HOURLY CHARGE INCLUDING ASSOCIATED CHILDREN
            if 'hourlyRecurringFee' in item:
                hourlyRecurringFee = round(float(item['hourlyRecurringFee']),3)
                creditAmount = round(float(item['hourlyRecurringFee']) * actualCreditHours,2)
            else:
                hourlyRecurringFee = 0
                creditAmount = 0

            for child in billing_detail:
                if 'hourlyRecurringFee' in child:
                    hourlyRecurringFee = round(hourlyRecurringFee + float(child['hourlyRecurringFee']),3)
                    creditAmount = round(creditAmount + round(float(child['hourlyRecurringFee']) * actualCreditHours,2),2)



            # FORMAT DATE & TIME STAMPS AND DELTAS FOR CSV
            createDate=datetime.strftime(createDateStamp,"%Y-%m-%d")
            createTime=datetime.strftime(createDateStamp,"%H:%M:%S")
            provisionDate=datetime.strftime(provisionDateStamp,"%Y-%m-%d")
            provisionTime=datetime.strftime(provisionDateStamp,"%H:%M:%S")
            provisionDelta=convert_timedelta(provisionDateStamp-createDateStamp)


            if cancellationDateStamp=="Running":
                cancellationDate="Running"
                cancellationTime="Running"
            else:
                cancellationDate=datetime.strftime(cancellationDateStamp,"%Y-%m-%d")
                cancellationTime=datetime.strftime(cancellationDateStamp,"%H:%M:%S")

            # Create CSV Record
            logging.warning('Writting record for InvoiceId %s, BillingItemId %s, GuestId %s.' % (invoiceID, billingItemId, guestId))
            row = {'InvoiceID': invoiceID,
                   'BillingItemId': billingItemId,
                   'TransactionID': guestId,
                   'Datacenter': location,
                   'Product': product,
                   'Cores': cores,
                   'OS': os,
                   'Memory': memory,
                   'Disk': disk,
                   'Hostname': hostName,
                   'CreateDate': createDate,
                   'CreateTime': createTime,
                   'ProvisionedDate': provisionDate,
                   'ProvisionedTime': provisionTime,
                   'ProvisionedDelta': provisionDelta,
                   'CancellationDate': cancellationDate,
                   'CancellationTime': cancellationTime,
                   'HoursUsed': hoursUsed,
                   'HourlyRecurringFee': hourlyRecurringFee,
                   'CreditHours': vsicredit,
                   'ActualCreditHours': actualCreditHours,
                   'CreditAmount': creditAmount
                   }

            csvwriter.writerow(row)

##close CSV File
logging.warning('Finished %s NEW/ONE-TIME Invoices between %s and %s.' % (len(InvoiceList),startdate,enddate))
outfile.close()
