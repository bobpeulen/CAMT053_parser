from sepa import parser
import re
import pandas as pd

def full_function(xml_file):
    
    def strip_namespace(xml):
        return re.sub(' xmlns="[^"]+"', '', xml, count=1)

    # Read file
    with open(xml_file, 'r') as f:
        input_data = f.read()

    # Parse the bank statement XML to dictionary
    camt_dict = parser.parse_string(parser.bank_to_customer_statement, bytes(strip_namespace(input_data), 'utf8'))

    statements = pd.DataFrame.from_dict(camt_dict['statements'])
    all_entries = []
    dd_all = []
    for i,_ in statements.iterrows():
        if 'entries' in camt_dict['statements'][i]:

            #create empty df
            df = pd.DataFrame()
            dd = pd.DataFrame.from_records(camt_dict['statements'][i]['entries']) 

            df['reference'] = dd['reference']
            df['credit_debit_indicator'] = dd['credit_debit_indicator']
            df['status'] = dd['status']
            df['account_servicer_reference'] = dd['account_servicer_reference']

            iban = camt_dict['statements'][i]['account']['id']['iban']
            name = camt_dict['statements'][i]['account']['name']
            df['iban'] = iban
            df['name'] = name
            df['currency'] = dd['amount'].str['currency']
            df['amount'] = dd['amount'].str['_value']
            df['reference'] = dd['reference']

            df['value_date'] = dd['value_date'].str['date']
            df['value_date'] = pd.to_datetime(df['value_date']).dt.strftime('%Y-%m-%d')
            df['booking_date'] = dd['booking_date'].str['date']
            df['booking_date'] = pd.to_datetime(df['booking_date']).dt.strftime('%Y-%m-%d')

            #bank transaction code
            df['proprietary_code'] = dd['bank_transaction_code'].str['proprietary'].str['code']
            df['proprietary_issuer'] = dd['bank_transaction_code'].str['proprietary'].str['issuer']

            df['domain_code'] = dd['bank_transaction_code'].str['domain'].str['code']
            df['family_code'] = dd['bank_transaction_code'].str['domain'].str['family'].str['code']
            df['sub_family_code'] = dd['bank_transaction_code'].str['domain'].str['family'].str['sub_family_code']

            #transaction details
            df['debtor_name'] = dd['entry_details'].str[0].str['transaction_details'].str[0].str['related_parties'].str['debtor'].str['name']
            df['debtor_iban'] = dd['entry_details'].str[0].str['transaction_details'].str[0].str['related_parties'].str['debtor_account'].str['id'].str['iban']
            df['creditor_name'] = dd['entry_details'].str[0].str['transaction_details'].str[0].str['related_parties'].str['creditor'].str['name']
            df['creditor_iban'] = dd['entry_details'].str[0].str['transaction_details'].str[0].str['related_parties'].str['creditor_account'].str['id'].str['iban']

            df['bic'] = dd['entry_details'].str[0].str['transaction_details'].str[0].str['related_agents'].str['debtor_agent'].str['financial_institution'].str['bic']
            df['remittance_information'] = dd['entry_details'].str[0].str['transaction_details'].str[0].str['remittance_information'].str['unstructured'].str[0]

            df['account_servicer_reference'] = dd['entry_details'].str[0].str['transaction_details'].str[0].str['refs'].str['account_servicer_reference']
            df['end_to_end_id'] = dd['entry_details'].str[0].str['transaction_details'].str[0].str['refs'].str['end_to_end_id']

        all_entries.append(df)

    df_entries = pd.concat(all_entries)

    #drop duplicates
    df_entries = df_entries.drop_duplicates(subset=['reference'], keep='last')
    
    return df_entries
