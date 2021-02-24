#!/usr/bin/env python
import logging
import pprint
from time import sleep

from ads.adsconnection import AdsConnection
from ads.adsclient import AdsClient
from ads.adssymbol import AdsSymbolList

def main():
    # *********************************************************************
    # Set your options:
    option_print_symbol_list = False
    option_print_result_sum_read = False
    option_print_result_block_read = True

    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s][%(process)d:%(threadName)s]"
        "[%(filename)s/%(funcName)s][%(levelname)s] %(message)s")

    ads_conn = AdsConnection(
        target_ams='5.0.0.0.1.1:851',
        target_ip='10.0.0.1',
        target_port=851,
        source_ams='10.33.0.1.1.1:32733',
    )

    with AdsClient(ads_conn, debug=False) as device:
        print("")
        print("DEVICE INFO")
        print("")
        pprint.pprint(device.read_device_info().__dict__)
        print("")
        print("SYMBOLS")
        print("")

        # get the types
        typeinfolist = device.get_types()
        # get the symbols on top level
        symbolinfolist = device.get_symbols()

        # Subsistute structs and arrays in the symbolinfolist with the typeinfolist
        symbollist  = AdsSymbolList(typeinfolist, symbolinfolist, alignment=True)

        # It's possible to print the complete symbol list
        if option_print_symbol_list:
            print('################### Complete Symbol List ########################')
            print(symbollist)
            sleep(1)
        else:
            print('Option Symbol List not set. Skipping')
            sleep(1)

        # But a filter is better. The syntax is for regular expressions. Get all parameters for winch 0
        if option_print_result_sum_read:
            print('################### Result of SUM_READ ########################')
            filter = ['config','machine\[0\]']
            mylist = symbollist.filter(filter)
            
            filter = ['sensor']
            mylist.extend_list(symbollist.filter(filter))

            # Read the values from the PLC
            device.sum_read(mylist)
            print(mylist)
        else:
            print('Option SUM_READ not set. Skipping')
            sleep(1)

        if option_print_result_block_read:
            print('################### Result of SUM_READ ########################')
            # Make a new list
            filter = ['parameters']

            # Instead of reading every single symbol, get a memory block and deserialize
            # This is cheaper
            mylist = symbollist.filter(filter)
            device.block_read(mylist)

            print(mylist)

        else:
            print('Option BLOCK_READ not set. Skipping')
            sleep(1)


if __name__ == '__main__':
    main()
    
