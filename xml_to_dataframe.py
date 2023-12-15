import gradio as gr
import pandas as pd
import numpy as np
from sepa import parser
import re

#####################################################################################################################################
#####################################################################################################################################
#####################################################################################################################################

def full_function(xml_file):
    
    #for gradio: swap with xml_file for local testing
    full_name = xml_file.name
    #full_name = xml_file

    
    print("File name in gradio is ")
    print(full_name)
    
    def strip_namespace(xml):
        return re.sub(' xmlns="[^"]+"', '', xml, count=1)

    # Read file
    with open(full_name, 'r') as f:
        input_data = f.read()

    # Parse the bank statement XML to dictionary
    print("Parse full xml string")
    camt_dict = parser.parse_string(parser.bank_to_customer_statement, bytes(strip_namespace(input_data), 'utf8'))

    statements = pd.DataFrame.from_dict(camt_dict['statements'])
    all_entries = []
    dd_all = []
    
    print("Start loop all the transactions and add to df")
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

    print("merge the list into df")
    df_entries = pd.concat(all_entries)

    #drop duplicates
    print("remove duplicate rows")
    df_entries = df_entries.drop_duplicates(subset=['reference'], keep='last')
    
    print("all done")
    
    df_entries_example = df_entries[['reference', 'credit_debit_indicator', 'iban', 'name', 'currency', 'amount', 'value_date', 'debtor_name', 'debtor_iban', 'creditor_name', 'creditor_iban', 'remittance_information']].head(20)
    #print(df_entries_example)
    
    return df_entries, df_entries_example

#####################################################################################################################################
#####################################################################################################################################
#####################################################################################################################################

def function_code_count(df_entries):
    
    #count number of values
    df_proprietary_code_count = df_entries['proprietary_code'].value_counts()#.to_frame()
    df_proprietary_code_count = pd.DataFrame(df_proprietary_code_count).reset_index(names="code")
    df_proprietary_code_count.rename(columns={"proprietary_code": "count"}, inplace=True)
    
    return df_proprietary_code_count

#####################################################################################################################################
#####################################################################################################################################
#####################################################################################################################################


def export_csv(xml_file):
    
    df_entries, df_entries_example = full_function(xml_file)
    
    df_entries.to_csv("./output.csv")
    
    out = gr.File.update(value="output.csv", visible=True)
    
    #count codes 
    df_proprietary_code_count = function_code_count(df_entries)
    
    return out, df_entries_example, df_proprietary_code_count

#####################################################################################################################################
#####################################################################################################################################
#####################################################################################################################################



desc = "Upload XML file, convert to .csv file, and analyze transactions"

with gr.Blocks() as demo: 

    xml_file = gr.File(label = "Upload XML file here")
     
    #output table. 
    df_entries_example = gr.DataFrame(label="Example output table, top 20 rows (not all columns)")
    
    with gr.Row():
        #export_button = gr.Button("Export")
        out = gr.File(label = "Output file", interactive=False, visible=False)
        
    with gr.Row():
        
        df_proprietary_code_count = gr.DataFrame(label="Number of transactions per code")


    #submit_btn = gr.Button("Run analysis on XML file")
    #export_button.click(export_csv, df_entries, csv)


gr.Interface(fn=export_csv, inputs=xml_file, outputs=[out, df_entries_example, df_proprietary_code_count], title=desc).launch(share=True, debug =True)
