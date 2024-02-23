import os
import time
import pandas as pd

# Import the necessary module from the Moralis library
from moralis import evm_api

your_wallet_address = '<YOUR WALLET ADDRESS>'

# Rarible's marketplace contract, to be filtered out
rarible = '0x12b3897a36fdb436dde2788c06eff0ffd997066e'

# Replace "YOUR_API_KEY" with your actual Moralis API key
api_key = "<YOUR API KEY>"


def index():

    # load contracts.csv, and convert the "contract" into  a dict, with the contract as the key and the name column as the value
    contracts = pd.read_csv('contracts.csv')
    contracts['contract'] = contracts['contract'].str.lower()
    contracts = contracts.set_index('contract').to_dict()['name']

    files = [
        # a list of files containing internal transactions
        'list-of-internals', 
    ]
    for file in files:
        txs = pd.read_csv(f'{file}.csv',dtype={'Txhash':str,})

        print(txs['Txhash'])

        results = {}

        # for each key in contracts, add a key to the results dict with the value being a float of 0
        for key in contracts:
            results[key] = 0.0

        def save():
            final = pd.DataFrame(list(results.items()), columns=['contract', 'value'])
            # add a column called "name", and map the contract to the name

            final['name'] = final['contract'].map(contracts)
            final.to_csv(f'{file}_values.csv', index=False)

            # delay for 0.5 seconds
            time.sleep(0.5)

        index_at = 1
        # iterate over each row
        for index, row in txs.iterrows():
            print('Doing row', index_at, 'of', len(txs))
            index_at += 1
            # get the value from the 'tx_hash' column, handle some odd edge case here
            tx_hash = row['Txhash'] if len(row['Txhash']) > 20 else row.name
            # Specify parameters, including the transaction hash and blockchain (e.g., Ethereum)
            params = {
                "transaction_hash": tx_hash,
                "chain": "polygon",
            }

            # Use the Moralis EVM API to get internal transactions associated with the specified hash, can be anything else however.
            result = evm_api.transaction.get_internal_transactions(
                api_key=api_key,
                params=params
            )
                
            # convert all from and to to lowercase
            result = pd.DataFrame(result)
            result['to'] = result['to'].str.lower()
            result['from'] = result['from'].str.lower()

            # provides a snapshot of each run for debugging purposes
            result.to_csv('result.csv', index=False)

            # get value for row where "to" equals the contract address
            value = result[result['to'] == your_wallet_address]
            # capture value of the "value" column
            if len(value) == 0:
                print('no match')
                save()
                continue
            value = float(value.iloc[0]['value'])
                
            # find and element where either to, or from is a key in the contracts dict
            results_to = result[result['to'].isin(contracts.keys())]
            results_from = result[result['from'].isin(contracts.keys())]

            if len(results_to) == 0:
                if len(results_from) == 0:
                    print('no match')
                    save()
                    continue
                collection = results_from.iloc[0]['from']
            else:
                collection = results_to.iloc[0]['to']

            to = collection
            if to in results:
                results[to] = float(results[to]) + float(value)
            else:    
                results[to] = float(value)

            save()

    return


index()

    
